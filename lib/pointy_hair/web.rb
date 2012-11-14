require 'sinatra/base'
require 'erb'
require 'pointy_hair'
require 'time'

if defined? Encoding
  Encoding.default_external = Encoding::UTF_8
end

module PointyHair
  class Web < Sinatra::Base
    attr_accessor :base_dir, :instance
    dir = File.dirname(File.expand_path(__FILE__))

    set :views,  "#{dir}/web/view"

    if respond_to? :public_folder
      set :public_folder, "#{dir}/web/public"
    else
      set :public, "#{dir}/web/public"
    end

    set :static, true

    helpers do
      include Rack::Utils
      alias_method :h, :escape_html

      def current_section
        url_path request.path_info.sub('/','').split('/')[0].downcase
      end

      def current_page
        url_path request.path_info.sub('/','')
      end

      def url_path(*path_parts)
        [ path_prefix, path_parts ].join("/").squeeze('/')
      end
      alias_method :u, :url_path

      def path_prefix
        request.env['SCRIPT_NAME']
      end

      def class_if_current(path = '')
        'class="current"' if current_page[0, path.size] == path
      end

      def tab(name)
        dname = name.to_s.downcase
        path = url_path(dname)
        "<li #{class_if_current(path)}><a href='#{path}'>#{name}</a></li>"
      end

      def tabs
        [ :foo, :bar ]
      end
    end

    def show(page, layout = true)
      response["Cache-Control"] = "max-age=0, private, must-revalidate"
      begin
        erb page.to_sym, {:layout => layout}, :manager => manager
      rescue ::Interrupt
        raise
      rescue ::Exception => err
        erb :error, {:layout => false}, :error => "Error: #{err.inspect}"
      end
    end

    def show_for_polling(page)
      content_type "text/html"
      @polling = true
      show(page.to_sym, false).gsub(/\s{1,}/, ' ')
    end

    # to make things easier on ourselves
    get "/?" do
      redirect url_path(:overview)
    end

    %w( overview workers ).each do |page|
      get "/#{page}.poll/?" do
        show_for_polling(page)
      end

      get "/#{page}/:kind/:instance/poll/?" do
        show_for_polling(page)
      end
    end

    %w( overview queues working workers key ).each do |page|
      get "/#{page}/?" do
        show page
      end

      get "/#{page}/:id/?" do
        show page
      end
    end

    post "/queues/:id/remove" do
      Resque.remove_queue(params[:id])
      redirect u('queues')
    end

    get "/failed/?" do
      if Resque::Failure.url
        redirect Resque::Failure.url
      else
        show :failed
      end
    end

    def manager
      @manager ||=
        PointyHair::Manager.new(:base_dir => base_dir, :instance => instance).
        infer_pid!.
        get_state!
    end

    def self.tabs
      @tabs ||= ["Overview", "Working", "Failed", "Queues", "Workers", "Stats"]
    end

  end
end
