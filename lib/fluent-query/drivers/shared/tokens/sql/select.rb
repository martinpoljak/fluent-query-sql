# encoding: utf-8
require "fluent-query/drivers/shared/tokens/sql"

module FluentQuery
    module Drivers
        module Shared
            module Tokens
                module SQL
                                    
                     ##
                     # Generic SQL query SELECT token.
                     #
                     
                     class Select < FluentQuery::Drivers::Shared::Tokens::SQLToken

                        ##
                        # Renders this token.
                        #

                        public
                        def render!(mode = :build)
                            stack = [ ]
                            unknown = [ ]
                            fields = [ ]
                            aliases = { }
                            distinct = false
                            
                            result = "SELECT "
                            processor = @_query.processor
                            
                            _class = self.unknown_token

                            # Process subtokens
                            
                            @_subtokens.each do |token|
                                arguments = token.arguments
                                name = token.name

                                # Known select process
                                if (name == :select) or (name == :distinct)
                                    length = arguments.length
                                    
                                    if length > 0
                                        first = arguments.first
                                        
                                        if first.kind_of? Symbol
                                            stack << arguments
                                        elsif first.kind_of? String
                                            stack << processor.process_formatted(arguments, mode)
                                        else
                                            arguments.each do |argument|
                                                if argument.kind_of? Array
                                                    fields += argument
                                                elsif argument.kind_of? Hash
                                                    aliases.merge! argument
                                                end                        
                                            end
                                        end
                                    end

                                    # Closes opened native token with unknown tokens
                                    if unknown.length > 0
                                        native = _class::new(@_driver, @_query, unknown)
                                        stack << native.render!
                                        unknown = [ ]
                                    end

                                # Unknowns arguments pushes to the general native token
                                else
                                    unknown << token
                                end

                                distinct = (name == :distinct)
                            end

                            # Closes opened native token with unknown tokens
                            if unknown.length > 0
                                native = _class::new(@_driver, @_query, unknown)
                                stack << native.render!
                                unknown = [ ]
                            end

                            # process distinct
                            if distinct
                                result << "DISTINCT "
                            end

                            # Process stack with results
                            
                            first = true
                                                        
                            if not fields.empty?
                                stack.unshift(fields.uniq)
                            end
                            
                            if not aliases.empty?
                                stack.unshift(aliases)
                            end

                            stack.each do |item|
                                
                                if item.kind_of? Array
                            
                                    if not first
                                        result << ", "
                                    end
                                    
                                    result << processor.process_identifiers(item) 
                                    first = false
                                    
                                elsif item.kind_of? Hash
                                    fields = [ ]
                                    
                                    item.each_pair do |key, value|
                                        key = @_driver.quote_identifier(key)
                                        value = @_driver.quote_identifier(value)
                                        
                                        fields << (key.to_s + " AS " + value.to_s)
                                    end

                                    if not first
                                        result << ", "
                                    end
                                    
                                    result << fields.join(", ")
                                    first = false
                                    
                                elsif item.kind_of? String
                            
                                    if not first
                                        result << ", "
                                    end
        
                                    result << item
                                    first = false
                                    
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

