# encoding: utf-8
require "fluent-query/drivers/shared/tokens/sql"
require "fluent-query/drivers/exception"

module FluentQuery
    module Drivers
        module Shared
            module Tokens
                module SQL
                
                     ##
                     # Generic SQL query INSERT token.
                     #
 
                     class Insert < FluentQuery::Drivers::Shared::Tokens::SQLToken
                     
                        ##
                        # Renders this token.
                        #

                        public
                        def render!(mode = nil)
                            processor = @_query.processor
                            result = "INSERT INTO "
                        
                            @_subtokens.each do |token|
                                arguments = token.arguments

                                # INSERT token
                                if token.name == :insert

                                    # Checks for arguments
                                    if (not arguments[0].kind_of? Symbol) or (not arguments[1].kind_of? Hash)
                                       raise FluentQuery::Drivers::Exception::new("Symbol and Hash arguments expected for #insert method.")
                                    end

                                    # Process
                                    table = processor.quote_identifier(arguments[0])
                                    fields = processor.process_identifiers(arguments[1].keys)
                                    values = processor.process_array(arguments[1].values)
                                    
                                    result << table << " (" << fields << ") VALUES (" << values << ")"

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

