package org.example

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors

const val COMPUTE_LOOP_COUNT_MIN =  10_000  // 1処理のループ回数の最小値
const val COMPUTE_LOOP_COUNT_MAX = 100_000  // 1処理のループ回数の最大値
const val REST_COUNT = 20 // 1処理の間に、これだけの回数だけ休憩を取る
const val REST_MILLIS = 10L // 1回の休憩の休む時間

const val NUM_COMPUTE = 100_000 // いくつの処理を実行するか

const val FIXED_THREAD_POOL_SIZE = 512 // Fixedスレッドプールの数

const val NUM_TRIAL = 5 // 全体を何回やってみるか

// 指定されたループ回数だけ、処理を実行する
// その間に、REST_COUNT回数だけ休憩を取る
// 1回の休憩は REST_MILLIS ミリ秒
inline fun compute(loopCount: Int, rest: () -> Unit) {
    val restTiming = loopCount / REST_COUNT
    var count = 0
    var restSum = 0
    repeat(loopCount) {
        count += 1
        
        // 休憩のタイミングになったら休む
        if (count >= restTiming) {
            count = 0
            val restStart = Instant.now()
            rest()
            val restEnd = Instant.now()
            restSum += Duration.between(restStart, restEnd).toMillis().toInt()
        }
    }
    
    // 休憩時間が短すぎる場合は手を挙げなさい（挙げる人はいないはずなので一応チェック）
    if (restSum < REST_MILLIS * REST_COUNT) {
        println("Rest time is too short: $restSum ms")
    }
}

// Thread.sleep で休憩するブロッキング関数
fun blockingCompute(loopCount: Int) {
    compute(loopCount) {
        Thread.sleep(REST_MILLIS)
    }
}

// delay で休憩するサスペンド関数
suspend fun suspendableCompute(loopCount: Int) {
    compute(loopCount) {
        delay(REST_MILLIS)
    }
}

// スレッドプールを使わず、シンプルにスレッドで実行
fun experiment1(loopCounts: List<Int>) {
    println("[Simple Thread]")
    measure {
        val latch = CountDownLatch(NUM_COMPUTE)
        for (i in 0 ..< NUM_COMPUTE) {
            Thread {
                blockingCompute(loopCounts[i])
                latch.countDown()
            }.start()
        }
        println("${latch.count} jobs left.")
        latch.await()
    }
}

// スレッドプールを使って実行
// でも一気に処理を投げるので、プールが有効に活用されることはなさそう
// …と思ったのだが、処理を投げているうちに前に投げたスレッドが終わったらそれが使われているよう
fun experiment2(loopCounts: List<Int>) {
    val exec = Executors.newCachedThreadPool()
    println("[Cached Thread Pool]")
    measure {
        val latch = CountDownLatch(NUM_COMPUTE)
        for (i in 0 ..< NUM_COMPUTE) {
            exec.submit {
                blockingCompute(loopCounts[i])
                latch.countDown()
            }
        }
        println("${latch.count} jobs left.")
        latch.await()
    }
    exec.shutdown()
}

// 上限のあるスレッドプールを使って実行
fun experiment3(loopCounts: List<Int>) {
    val exec = Executors.newFixedThreadPool(FIXED_THREAD_POOL_SIZE)
    println("[Fixed Thread Pool]")
    measure {
        val latch = CountDownLatch(NUM_COMPUTE)
        for (i in 0 ..< NUM_COMPUTE) {
            exec.submit {
                blockingCompute(loopCounts[i])
                latch.countDown()
            }
        }
        println("${latch.count} jobs left.")
        latch.await()
    }
    exec.shutdown()
}

// コルーチンを引数なしのrunBlockingで実行
fun experiment4(loopCounts: List<Int>) {
    println("[Coroutine Empty Dispatcher]")
    measure {
        val latch = CountDownLatch(NUM_COMPUTE)
        runBlocking {
            for (i in 0 ..< NUM_COMPUTE) {
                launch {
                    suspendableCompute(loopCounts[i])
                    latch.countDown()
                }
            }
            println("${latch.count} jobs left.")
        }
    }
}

// コルーチンをDispatchers.DefaultのrunBlockingで実行
fun experiment5(loopCounts: List<Int>) {
    println("[Coroutine Default Dispatcher]")
    measure {
        val latch = CountDownLatch(NUM_COMPUTE)
        runBlocking(Dispatchers.Default) {
            for (i in 0 ..< NUM_COMPUTE) {
                launch {
                    suspendableCompute(loopCounts[i])
                    latch.countDown()
                }
            }
            println("${latch.count} jobs left.")
        }
    }
}

// コルーチンをDispatchers.IOのrunBlockingで実行
fun experiment6(loopCounts: List<Int>) {
    println("[Coroutine IO Dispatcher]")
    measure {
        val latch = CountDownLatch(NUM_COMPUTE)
        runBlocking(Dispatchers.IO) {
            for (i in 0 ..< NUM_COMPUTE) {
                launch {
                    suspendableCompute(loopCounts[i])
                    latch.countDown()
                }
            }
            println("${latch.count} jobs left.")
        }
    }
}

fun measure(block: () -> Unit) {
    val startInstant = Instant.now()
    block()
    val endInstant = Instant.now()
    Duration.between(startInstant, endInstant).also { duration ->
        println("Time taken: ${duration.toMillis()} ms")
    }
}

fun main() {
    repeat(NUM_TRIAL) {
        val loopCounts = (0..<NUM_COMPUTE).map {
            COMPUTE_LOOP_COUNT_MIN + (Math.random() * (COMPUTE_LOOP_COUNT_MAX - COMPUTE_LOOP_COUNT_MIN)).toInt()
        }
        experiment1(loopCounts)
        experiment2(loopCounts)
        experiment3(loopCounts)
        experiment4(loopCounts)
        experiment5(loopCounts)
        experiment6(loopCounts)
        println("--------------------------------")
    }
}
