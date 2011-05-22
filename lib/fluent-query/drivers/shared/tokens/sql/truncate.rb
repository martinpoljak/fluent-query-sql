# encoding: utf-8
require "fluent-query/drivers/shared/tokens/sql"
require "fluent-query/drivers/exception"

module FluentQuery
    module Drivers
        module Shared
            module Tokens
                module SQL
                                   
                     ##
                     # Generic SQL query TRUNCATE token.
                     #
                     
                     class Truncate < FluentQuery::Drivers::Shared::Tokens::SQLToken

                        ##
                        # Renders this token.
                        #

                        public
                        def render!(mode = nil)
                            processor = @_query.processor
                            result = "TRUNCATE TABLE "
                        
                            @_subtokens.each do |token|
                                arguments = token.arguments

                                # FROM token
                                
                                if token.name == :truncate

                                    # Checks for arguments
                                    if (not arguments.first.kind_of? Symbol)
                                        raise FluentQuery::Drivers::Exception::new("Symbol argument expected for #truncate method.")
                                    end

                                    # Process
                                    table = processor.quote_identifier(arguments.first)
                                    result << table

                                # Unknown tokens renders directly
                                else
                                    result << self.unknown_token::new(@_driver, @_query, [token]).render!
                                end
                                
                            end

                            result << " CASCADE"
                            return result
                        end
                    end
                end
            end
        end
    end
end

