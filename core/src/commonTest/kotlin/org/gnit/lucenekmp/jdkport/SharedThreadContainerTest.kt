package org.gnit.lucenekmp.jdkport

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

class SharedThreadContainerTest {

    @Test
    fun createAndName_parentRoot() {
        val container = SharedThreadContainer.create("pool-A")
        assertEquals("pool-A", container.name())
        // parent of shared container is root
        assertEquals(ThreadContainers.root(), container.parent())
        // toString has name/id
        assertTrue(container.toString().startsWith("pool-A/"))
    }

    @Test
    fun startRegistersThreadAndThreadsIterable() = runBlocking {
        val container = SharedThreadContainer.create("C1")

        // start a coroutine Job and register via JLA.start
        val job = launch { /* idle */ }
        // starting associates job with container and notifies onStart
        container.start(job)

        // ThreadContainers.container should resolve to this container
        assertEquals(container, ThreadContainers.container(job))

        // threads() should contain our job (jobs treated as platform threads here)
        val threads = container.threads().toList()
        assertTrue(threads.contains(job))

        job.cancel()
    }

    @Test
    fun closePreventsFurtherStart_butDoesNotAffectExisting() = runBlocking {
        val container = SharedThreadContainer.create("C2")
        val job = launch { }
        container.start(job)

        // closing should deregister; future starts should fail
        container.close()

        assertFailsWith<IllegalStateException> {
            container.start(launch { })
        }

        // Existing job remains associated and visible until completion
        assertEquals(container, ThreadContainers.container(job))
        assertTrue(container.threads().toList().contains(job))

        job.cancel()
    }

    @Test
    fun parentAndChildrenRelationships() = runBlocking {
        val parent = SharedThreadContainer.create("P")
        val child = SharedThreadContainer.create(parent, "C")

        assertEquals(parent, child.parent())
        val children = parent.children().toList()
        // child should appear among children (registered containers + threads' top containers)
        assertTrue(children.contains(child))

        // Root has no parent
        assertNull(ThreadContainers.root().parent())
    }

    @Test
    fun threadCountReflectsPlatformThreadsInContainer() = runBlocking {
        val container = SharedThreadContainer.create("CNT")
        val j1 = launch { }
        val j2 = launch { }
        container.start(j1)
        container.start(j2)

        // threadCount counts live threads from threads() sequence
        val count = container.threadCount()
        assertTrue(count >= 2)

        j1.cancel(); j2.cancel()
    }

    @Test
    fun concurrentRegistryLifecycleAndStaleQueueCleanup() = runBlocking {
        withTimeout(60_000) {
            val parent = SharedThreadContainer.create("concurrent-parent")
            try {
                coroutineScope {
                    repeat(8) { worker ->
                        launch(Dispatchers.Default) {
                            repeat(500) { iteration ->
                                val container = SharedThreadContainer.create(parent, "$worker-$iteration")
                                try {
                                    assertTrue(ThreadContainers.children(parent).any { it === container })
                                    ThreadContainers.children(ThreadContainers.root()).toList()
                                } finally {
                                    container.close()
                                    container.close()
                                }
                            }
                        }
                    }
                }
                assertTrue(ThreadContainers.children(parent).none())
                val key = ThreadContainers.registerContainer(parent) as WeakReference<*>
                assertTrue(key.enqueue())
                ThreadContainers.children(ThreadContainers.root()).toList()
                assertNull(key.queue?.poll())
            } finally {
                parent.close()
            }
        }
    }

    @Test
    fun concurrentQueueEnqueuePollAndSnapshotCallbacks() = runBlocking {
        withTimeout(60_000) {
            val queue = ReferenceQueue<Int>()
            val consumed = mutableListOf<Int>()
            val consumedLock = ReentrantLock()
            coroutineScope {
                repeat(8) { worker ->
                    launch(Dispatchers.Default) {
                        repeat(500) { iteration ->
                            queue.enqueue(Reference(worker * 500 + iteration))
                            queue.forEach { it.get() }
                            queue.poll()?.get()?.let { value ->
                                consumedLock.withLock { consumed.add(value) }
                            }
                        }
                    }
                }
            }
            while (true) {
                val ref = queue.poll() ?: break
                consumed.add(ref.get()!!)
            }
            assertEquals((0 until 4000).toSet(), consumed.toSet())
            assertEquals(4000, consumed.size)
            queue.enqueue(Reference(1))
            queue.enqueue(Reference(2))
            val snapshot = mutableListOf<Int?>()
            queue.forEach { snapshot.add(it.get()); queue.poll() }
            assertEquals(listOf<Int?>(1, 2), snapshot)
            assertNull(queue.poll())
        }
    }

    @Test
    fun concurrentCloseIsIdempotent() = runBlocking {
        val container = SharedThreadContainer.create("concurrent-close")
        coroutineScope {
            repeat(16) { launch(Dispatchers.Default) { repeat(100) { container.close() } } }
        }
        assertTrue(ThreadContainers.children(ThreadContainers.root()).none { it === container })
    }

    @Test
    fun concurrentTrackingRootLifecycleAndSnapshots() = runBlocking {
        withTimeout(60_000) {
            val root = ThreadContainers.RootContainer.TrackingRootContainer()
            val baseline = root.threadCount()
            coroutineScope {
                repeat(8) {
                    launch(Dispatchers.Default) {
                        repeat(500) {
                            val job = kotlinx.coroutines.Job()
                            try {
                                root.onStart(job)
                                assertTrue(root.threads().any { it === job })
                                root.threadCount()
                            } finally {
                                root.onExit(job)
                                job.cancel()
                            }
                        }
                    }
                }
            }
            assertEquals(baseline, root.threadCount())
        }
    }
}
