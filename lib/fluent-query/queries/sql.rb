# encoding: utf-8
require "fluent-query/query"

module FluentQuery
    module Queries
                
         ##
         # Represents SQL query.
         # It ensures calls occupied by Ruby. (For example 'select()'.)
         #
         
         class SQL < FluentQuery::Query

            NONALPHA_REPLACER = /[^a-zA-Z0-9]/

            ##
            # Catches missing methods calls. Converts them to tokens.
            # In token names starting with '_' skips non-alfanumeric characters.
            #

            def method_missing(sym, *args, &block)
                # If name starts with underscore, strips non-alfa numeric characters out
                sym = sym.to_s
                
                if sym[0].ord == 95  # "_"
                    sym.gsub!(self.class::NONALPHA_REPLACER, "")
                end
                
                return super(sym.to_sym, *args, &block)
            end
            
            ##
            # Ensures processor call of parent classes which isn't
            # handled well from unknown reason via extending and 
            # falls to method_missing(...).
            #
            
            def processor
                super
            end
            
            ##
            # Ensures 'select()' call occupied by Ruby.
            #
            
            def select(*args)
                self.push_token(:select, args)
                return self
            end
            
         end

    end
end
