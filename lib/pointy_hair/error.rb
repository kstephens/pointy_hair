# -- encoding : utf-8 --
module PointyHair
  class Error < ::Exception
    class Stop < self; end
  end
end
