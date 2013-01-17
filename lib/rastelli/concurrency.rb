module Rastelli
  module Concurrency
    java_import 'java.lang.Thread'
    java_import 'java.lang.InterruptedException'
    java_import 'java.util.concurrent.atomic.AtomicInteger'
    java_import 'java.util.concurrent.atomic.AtomicBoolean'
    java_import 'java.util.concurrent.ThreadFactory'
    java_import 'java.util.concurrent.Executors'
    java_import 'java.util.concurrent.LinkedBlockingQueue'
    java_import 'java.util.concurrent.LinkedBlockingDeque'
    java_import 'java.util.concurrent.ArrayBlockingQueue'
    java_import 'java.util.concurrent.TimeUnit'
    java_import 'java.util.concurrent.CountDownLatch'
    java_import 'java.util.concurrent.locks.ReentrantLock'
    java_import 'java.util.concurrent.ConcurrentHashMap'
  end

  class Lock
    
    def initialize
      @lock = Concurrency::ReentrantLock.new
    end

    def lock
      begin
        @lock.lock
        yield
      ensure
        @lock.unlock
      end
    end
  end
end
