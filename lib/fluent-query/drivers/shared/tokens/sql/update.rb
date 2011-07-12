# encoding: utf-8
require "fluent-query/drivers/shared/tokens/sql"
require "fluent-query/drivers/exception"
require "hash-utils/object"   # >= 0.17.0

module FluentQuery
    module Drivers
        module Shared
            module Tokens
                module SQL
                
                     ##
                     # Generic SQL query UPDATE token.
                     #
                     
                     class Update < FluentQuery::Drivers::Shared::Tokens::SQLToken

                        ##
                        # Renders this token.
                        #

                        public
                        def render!(mode = nil)
                        
                            processor = @_query.processor
                            result = "UPDATE "
                        
                            @_subtokens.each do |token|
                                arguments = token.arguments

                                # UPDATE token
                                if token.name == :update

                                    # Checks for arguments
                                    if (not arguments.first.symbol?)
                                        raise FluentQuery::Drivers::Exception::("Symbol arguments expected for #update method.")
                                    end

                                    # Process
                                    table = processor.quote_identifier(arguments.first)
                                    result << table

                                # Unknown tokens renders directly
                                else
                                    result = self.unknown_token::new(@_driver, @_query, token).render!
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

