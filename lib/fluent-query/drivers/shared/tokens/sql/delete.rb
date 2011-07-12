# encoding: utf-8
require "hash-utils/object"   # >= 0.17.0
require "fluent-query/drivers/shared/tokens/sql"
require "fluent-query/drivers/exception"

module FluentQuery
    module Drivers
        module Shared
            module Tokens
                module SQL
                
                     ##
                     # Generic SQL query UPDATE token.
                     #
                     
                     class Delete < FluentQuery::Drivers::Shared::Tokens::SQLToken

                        ##
                        # Renders this token.
                        #

                        public
                        def render!(mode = nil)
                            processor = @_query.processor
                            result = "DELETE"
                        
                            @_subtokens.each do |token|
                                arguments = token.arguments

                                # FROM token
                                
                                if token.name == :from
                                    first = arguments.first

                                    # Checks for arguments
                                    if (not first.symbol?)
                                        raise FluentQuery::Drivers::Exception::new("Symbol argument expected for #from method.")
                                    end

                                    # Process
                                    table = processor.quote_identifier(first.to_s)
                                    result << "FROM " << table

                                # Unknown tokens renders directly
                                else
                                    result = self.unknown_token::new(@_driver, @_query, [token]).render!
                                end
                            end

                            return result
                        end
                    end
                end
            end
        end
    end
end

