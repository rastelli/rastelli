require 'rastelli/producer/partitioner'

module Rastelli
	class Producer

		def initialize(zk_hosts, partitioner, options = {})
			@zk_hosts = zk_hosts
			@partitioner = partitioner 

			@batch_options = options[:batch] || { :size => 1 }
			@buffers = @partitioner.streams.inject({}) do |hash, stream|
				hash[stream] = Concurrency::LinkedBlockingDeque.new
				hash
			end

			# should probably move this out of Producer and inject it instead,
			# just can't be bothered right now
			@internal_producer = Heller::Producer.new(@zk_hosts)
		end

		def start!
			if drain_timeout?
				@scheduler = Concurrency::Executors.new_single_thread_scheduled_executor
				@drainer_task = @scheduler.schedule_with_fixed_delay(
					method(:maybe_force_drain).to_proc,
					drain_timeout,
					drain_timeout,
					Concurrency::TimeUnit::SECONDS
				)
				@last_drain = Time.now
			end

			self
		end

		def produce(key, message)
			stream = @partitioner.get(key)

			unless stream.nil?
				@buffers[stream].add_last(message)

				drain_batch(stream, @buffers[stream]) if drain_buffer?(stream)
			else
				# log nil stream
				# throw away messages or send them to a special topic?
			end
		end

		def broadcast(message)
			@buffers.values.each { |buf| buf.add_last(message) }
			drain
		end

		def flush!
			drain(force = true)
		end

		def disconnect
			@drainer_task.cancel(may_interrupt = false) if @drainer_task
			@scheduler.shutdown if @scheduler
			@internal_producer.close
		end

		private

		def drain_buffer?(stream, force = false)
			(@buffers[stream].size >= batch_size) || (@buffers[stream].any? && force)
		end

		def drain(force = false)
			@buffers.each do |stream, buffer|
				while drain_buffer?(stream, force)
					drain_batch(stream, buffer)
				end
			end

			@last_drain = Time.now
		end

		def drain_batch(stream, buffer)
			batch = []
			
			batch_size.times do
				msg = buffer.poll_first
				batch << msg if msg
			end

			@internal_producer.produce(stream.to_s => { :messages => batch })
		end

		def maybe_force_drain
			return if (Time.now - @last_drain) < drain_timeout
			drain(force = true)
		end

		def drain_timeout
			@batch_options[:timeout]
		end

		def drain_timeout?
			drain_timeout && drain_timeout > 0
		end

		def batch_size
			@batch_options[:size]
		end
	end
end
