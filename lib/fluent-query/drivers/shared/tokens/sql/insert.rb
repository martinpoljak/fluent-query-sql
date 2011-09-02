# encoding: utf-8
require "fluent-query/drivers/shared/tokens/sql"
require "fluent-query/drivers/exception"
require "hash-utils/object"   # >= 0.17.0
require "hash-utils/array"

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
                                    values = arguments.second.values

                                    # Checks for arguments
                                    if (not arguments.first.symbol?) or (not arguments.second.hash?)
                                       raise FluentQuery::Drivers::Exception::new("Symbol and Hash arguments expected for #insert method.")
                                    end

                                    # Process
                                    table = processor.quote_identifier(arguments.first)
                                    fields = processor.process_identifiers(arguments.second.keys)
                                    
                                    if mode == :prepare
                                        values = values.map do |item|
                                            if item != ??
                                                processor.quote_value(i)
                                            else
                                                item
                                            end
                                        end
                                        
                                        values = values.join(', ')
                                    else
                                        values = processor.process_array(values)
                                    end
                                    
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

