# encoding: utf-8
require "fluent-query/drivers/shared/tokens/sql"
require "fluent-query/drivers/exception"

module FluentQuery
    module Drivers
        module Shared
            module Tokens
                module SQL
            
                     ##
                     # Generic SQL query SET token.
                     #
                     
                     class Set < FluentQuery::Drivers::Shared::Tokens::SQLToken

                        ##
                        # Renders this token.
                        #

                        public
                        def render!(mode = :build)
                            _class = self.unknown_token
                            
                            stack = [ ]
                            unknown = [ ]

                            operator = @_driver.quote_operator(:and)
                            processor = @_query.processor
                            result = "SET "
                        
                            @_subtokens.each do |token|
                                arguments = token.arguments

                                # SET token
                                
                                if token.name == :set
                                    length = arguments.length

                                    # Checks for arguments
                                    if length > 0
                                        if (length > 1) or (arguments.first.kind_of? String)
                                            stack << processor.process_formatted(arguments, mode)
                                        elsif arguments.first.kind_of? Hash
                                            stack << processor.process_hash(arguments.first, ", ", :assigning)
                                        end
                                    else
                                        raise FluentQuery::Drivers::Exception::new("SET token expects Hash or formatted string as argument.")
                                    end

                                # Unknown tokens renders directly
                                else
                                    result << _class::new(@_driver, @_query, [token]).render!
                                end
                                
                            end


                            # Closes opened native token with unknown tokens
                            if unknown.length > 0
                                native = _class::new(@_driver, @_query, unknown)
                                stack << native
                                unknown = [ ]
                            end

                            # Process stack with results
                            first = true
                            
                            stack.each do |item|
                                if item.kind_of? _class
                                    result << item.render!
                                elsif item.kind_of? String
                                    if not first
                                        result << operator << " "
                                    else
                                        first = false
                                    end
                                    
                                    result << item
                                end

                                result << " "
                            end

                            return result
                        end
                    end
                end
            end
        end
    end
end

