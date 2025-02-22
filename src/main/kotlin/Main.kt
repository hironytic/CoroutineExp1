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
inline fun compute(loopCount: Int, idSet: MutableSet<Long>, rest: () -> Unit) {
    synchronized(idSet) {
        idSet.add(Thread.currentThread().id)
    }
    
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
            
            synchronized(idSet) {
                idSet.add(Thread.currentThread().id)
            }
        }
    }
    
    // 休憩時間が短すぎる場合は手を挙げなさい（挙げる人はいないはずなので一応チェック）
    if (restSum < REST_MILLIS * REST_COUNT) {
        println("Rest time is too short: $restSum ms")
    }
}

// Thread.sleep で休憩するブロッキング関数
fun blockingCompute(loopCount: Int, idSet: MutableSet<Long>) {
    compute(loopCount, idSet) {
        Thread.sleep(REST_MILLIS)
    }
}

// delay で休憩するサスペンド関数
suspend fun suspendableCompute(loopCount: Int, idSet: MutableSet<Long>) {
    compute(loopCount, idSet) {
        delay(REST_MILLIS)
    }
}

// スレッドプールを使わず、シンプルにスレッドで実行
fun experiment1(loopCounts: List<Int>) = CollectedData().apply {
    println("[Simple Thread]")
    timeTaken = measure {
        val latch = CountDownLatch(NUM_COMPUTE)
        timeStartProcesses = measure {
            for (i in 0 ..< NUM_COMPUTE) {
                Thread {
                    blockingCompute(loopCounts[i], idSet)
                    latch.countDown()
                }.start()
            }
        }
        jobsLeft = latch.count
        latch.await()
    }
    printResult()
}

// スレッドプールを使って実行
// でも一気に処理を投げるので、プールが有効に活用されることはなさそう
// …と思ったのだが、処理を投げているうちに前に投げたスレッドが終わったらそれが使われているよう
fun experiment2(loopCounts: List<Int>) = CollectedData().apply {
    println("[Cached Thread Pool]")
    val exec = Executors.newCachedThreadPool()
    timeTaken = measure {
        val latch = CountDownLatch(NUM_COMPUTE)
        timeStartProcesses = measure {
            for (i in 0 ..< NUM_COMPUTE) {
                exec.submit {
                    blockingCompute(loopCounts[i], idSet)
                    latch.countDown()
                }
            }
        }
        jobsLeft = latch.count
        latch.await()
    }
    printResult()
}

// 上限のあるスレッドプールを使って実行
fun experiment3(loopCounts: List<Int>) = CollectedData().apply {
    println("[Fixed Thread Pool]")
    val exec = Executors.newFixedThreadPool(FIXED_THREAD_POOL_SIZE)
    timeTaken = measure {
        val latch = CountDownLatch(NUM_COMPUTE)
        timeStartProcesses = measure {
            for (i in 0 ..< NUM_COMPUTE) {
                exec.submit {
                    blockingCompute(loopCounts[i], idSet)
                    latch.countDown()
                }
            }
        }
        
        jobsLeft = latch.count
        latch.await()
    }
    exec.shutdown()
    printResult()
}

// コルーチンを引数なしのrunBlockingで実行
fun experiment4(loopCounts: List<Int>) = CollectedData().apply {
    println("[Coroutine Empty Dispatcher]")
    timeTaken = measure {
        val latch = CountDownLatch(NUM_COMPUTE)
        runBlocking {
            timeStartProcesses = measure {
                for (i in 0 ..< NUM_COMPUTE) {
                    launch {
                        suspendableCompute(loopCounts[i], idSet)
                        latch.countDown()
                    }
                }
            }
            jobsLeft = latch.count
        }
    }
    printResult()
}

// コルーチンをDispatchers.DefaultのrunBlockingで実行
fun experiment5(loopCounts: List<Int>) = CollectedData().apply {
    println("[Coroutine Default Dispatcher]")
    timeTaken = measure {
        val latch = CountDownLatch(NUM_COMPUTE)
        runBlocking(Dispatchers.Default) {
            timeStartProcesses = measure {
                for (i in 0 ..< NUM_COMPUTE) {
                    launch {
                        suspendableCompute(loopCounts[i], idSet)
                        latch.countDown()
                    }
                }
            }
            jobsLeft = latch.count
        }
    }
    printResult()
}

// コルーチンをDispatchers.IOのrunBlockingで実行
fun experiment6(loopCounts: List<Int>) = CollectedData().apply {
    println("[Coroutine IO Dispatcher]")
    timeTaken = measure {
        val latch = CountDownLatch(NUM_COMPUTE)
        runBlocking(Dispatchers.IO) {
            timeStartProcesses = measure {
                for (i in 0 ..< NUM_COMPUTE) {
                    launch {
                        suspendableCompute(loopCounts[i], idSet)
                        latch.countDown()
                    }
                }
            }
            jobsLeft = latch.count
        }
    }
    printResult()
}

// コルーチンをDispatchers.UnconfinedのrunBlockingで実行
fun experiment7(loopCounts: List<Int>) = CollectedData().apply {
    println("[Coroutine Unconfined Dispatcher]")
    timeTaken = measure {
        val latch = CountDownLatch(NUM_COMPUTE)
        runBlocking(Dispatchers.Unconfined) {
            timeStartProcesses = measure {
                for (i in 0 ..< NUM_COMPUTE) {
                    launch {
                        suspendableCompute(loopCounts[i], idSet)
                        latch.countDown()
                    }
                }
            }
            jobsLeft = latch.count
        }
    }
    printResult()
}

fun measure(block: () -> Unit): Long {
    val startInstant = Instant.now()
    block()
    val endInstant = Instant.now()
    return Duration.between(startInstant, endInstant).toMillis()
}

class CollectedData {
    val idSet = mutableSetOf<Long>()
    var jobsLeft: Long = 0
    var timeStartProcesses: Long = 0
    var timeTaken: Long = 0
    
    val numberOfThreads: Int
        get() = idSet.size
    
    fun printResult() {
        println("Time taken to start processes: $timeStartProcesses ms")
        println("Jobs left: $jobsLeft")
        println("Number of used threads: $numberOfThreads")
        println("Time taken for all: $timeTaken ms")
    }
}

data class AverageData(
    val averageJobsLeft: Double,
    val averageNumberOfThreads: Double,
    val averageTimeStartProcesses: Double,
    val averageTimeTaken: Double,
)

class MultipleTrialData(val label: String) {
    val trials = mutableListOf<CollectedData>()
    
    fun getAverage(): AverageData {
        val totalJobsLeft = trials.sumOf { it.jobsLeft }
        val totalNumberOfThreads = trials.sumOf { it.numberOfThreads }
        val totalTimeStartProcesses = trials.sumOf { it.timeStartProcesses }
        val totalTimeTaken = trials.sumOf { it.timeTaken }
        return AverageData(
            totalJobsLeft.toDouble() / trials.size,
            totalNumberOfThreads.toDouble() / trials.size,
            totalTimeStartProcesses.toDouble() / trials.size,
            totalTimeTaken.toDouble() / trials.size,
        )
    }
}

fun main() {
    val resultData = listOf(
        "Simple Thread",
        "Cached Thread Pool",
        "Fixed Thread Pool",
        "Coroutine Empty Dispatcher",
        "Coroutine Default Dispatcher",
        "Coroutine IO Dispatcher",
        "Coroutine Unconfined Dispatcher",
    ).map { MultipleTrialData(it) }
    
    repeat(NUM_TRIAL) {
        println("--------------------------------")
        val loopCounts = (0..<NUM_COMPUTE).map {
            COMPUTE_LOOP_COUNT_MIN + (Math.random() * (COMPUTE_LOOP_COUNT_MAX - COMPUTE_LOOP_COUNT_MIN)).toInt()
        }
        resultData[0].trials.add(experiment1(loopCounts))
        println()
        resultData[1].trials.add(experiment2(loopCounts))
        println()
        resultData[2].trials.add(experiment3(loopCounts))
        println()
        resultData[3].trials.add(experiment4(loopCounts))
        println()
        resultData[4].trials.add(experiment5(loopCounts))
        println()
        resultData[5].trials.add(experiment6(loopCounts))
        println()
        resultData[6].trials.add(experiment7(loopCounts))
        println()
    }
    
    print("Average:\t")
    print("Time taken to start\t")
    print("Jobs left\t")
    print("Number of used threads\t")
    print("Time taken for all\t")
    println()
    for (result in resultData) {
        val agerage = result.getAverage()
        print("${result.label}\t")
        print("${agerage.averageTimeStartProcesses}\t")
        print("${agerage.averageJobsLeft}\t")
        print("${agerage.averageNumberOfThreads}\t")
        print("${agerage.averageTimeTaken}\t")
        println()
    }
}
