# encoding: utf-8
require "fluent-query/query"
require "fluent-query/drivers/shared/tokens/sql"
require "fluent-query/drivers/exception"

module FluentQuery
    module Drivers
        module Shared
            module Tokens
                module SQL
                
                     ##
                     # Generic SQL query UNION token.
                     #
                     
                     class Union < FluentQuery::Drivers::Shared::Tokens::SQLToken

                        ##
                        # Renders this token.
                        #

                        public
                        def render!(mode = nil)
                            result = ""
                        
                            @_subtokens.each do |token|
                                arguments = token.arguments

                                # SET token
                                
                                if token.name == :union
                                    length = arguments.length

                                    # Checks for arguments
                                    if length >= 2
                                        queries = [ ]

                                        arguments.each do |argument|
                                            if argument.kind_of? String
                                                queries << argument
                                            elsif argument.kind_of? FluentQuery::Query
                                                queries << argument.build!
                                            end
                                        end
                                        
                                    else
                                        raise FluentQuery::Drivers::Exception::new("UNION token expects at least two queries or strings as arguments.")
                                    end
                                end            
                            end

                            # Process stack with results
                            queries.map! { |i| @_driver.quote_subquery(i) }
                            result << queries.join(" UNION ")

                            return result
                        end
                    end
                end
            end
        end
    end
end

