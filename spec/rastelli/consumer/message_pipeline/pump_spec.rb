require 'spec_helper'

module Rastelli

  describe MessagePipeline::Pump do

    let(:registry) do 
      mock(Registry).as_null_object
    end

    let(:source) { mock(Source).as_null_object }
    let(:masseur) { mock(Masseur).as_null_object }

    let(:pump) { Pump.new(registry, source, masseur) }

    describe '#process' do

      it 'fetches streams and offsets' do
        registry.should_receive(:streams_and_offsets).and_return()

        pump.process
      end

      it 'fetches messages' do
        source.should_receive(:fetch).and_return()

        pump.process
      end

      it 'updates registry with new offsets'

      it 'pushes messages further'

    end
  end
end
