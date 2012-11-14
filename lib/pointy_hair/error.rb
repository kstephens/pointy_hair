# -- encoding : utf-8 --
module PointyHair
  module Error
    class Base
      include Error
    end
    class Internal < Base; end
    class Stop < Base; end
    class Interrupt < ::Interrupt
      include Error
    end
  end
end
