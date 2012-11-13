# PointyHair

PointHair is a Ruby worker process manager.

It provides a robust framework for handling worker processes that loop: getting units of work, and working on them.
Control and status is supplied by simple files in a well-defined directory structure.
Signals sent to PointyHair::Manager processes are propagated to workers in a sane manner.

## Installation

Add this line to your application's Gemfile:

    gem 'pointy_hair'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install pointy_hair

## Usage


    mgr = PointyHair::Manager.new
    mgr.worker_config = {
      :kind_1 => {
        :class => 'PointyHair::Worker::Test',
        :enabled => true,
        :instances => 2,
        :options => {
          :a => 1,
          :b => 2,
        }
      }
    }
    mgr.run!

## TODO

* Support max_idle_time, worker #run_loop! should exit if #get_work! does not complete in max_idle_time.
* Worker process should spawn thread to check on reparenting, and call #stop! and/or abort current work.
* Support a simple web interface.

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
