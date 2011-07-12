# encoding: utf-8
require "fluent-query/drivers/shared/tokens/sql"
require "hash-utils/object"   # >= 0.17.0

module FluentQuery
    module Drivers
        module Shared
            module Tokens
                module SQL
                                    
                     ##
                     # Generic SQL query [LEFT|RIGHT|INNER|OUTER] JOIN token.
                     #
                     
                     class Join < FluentQuery::Drivers::Shared::Tokens::SQLToken

                        ##
                        # Renders this token.
                        #

                        public
                        def render!(mode = :build)
                            stack = [ ]
                            unknown = [ ]

                            _class = self.unknown_token
                            processor = @_query.processor
                            result = ""
                        
                            @_subtokens.each do |token|
                                arguments = token.arguments

                                # JOIN token
                                if token.name == :join
                                
                                    original_name = token.original_name
                                    stack << self._transform_token(original_name.to_s)
                            
                                    ##
                                    
                                    length = arguments.length
                                    first = arguments.first
                                    
                                    if length > 0
                                        if first.symbol?
                                            stack << processor.process_identifiers(arguments)
                                        elsif (length > 1) or (first.string?)
                                            stack << processor.process_formatted(arguments, mode)
                                        elsif first.hash?
                                            t_name = first.keys.first
                                            t_alias = first.values.first
                                            stack << (processor.quote_identifier(t_name) + " AS " + processor.quote_identifier(t_alias))
                                        end
                                    end

                                    # Closes opened native token with unknown tokens
                                    if unknown.length > 0
                                        native = _class::new(@_driver, @_query, unknown)
                                        stack << native
                                        unknown = [ ]
                                    end

                                # Unknown arguments pushes to the general native token
                                else
                                    unknown << token
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
                                elsif item.string?               
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

