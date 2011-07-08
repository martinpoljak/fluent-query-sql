# encoding: utf-8
require "fluent-query/drivers/shared/tokens/sql"

module FluentQuery
    module Drivers
        module Shared
            module Tokens
                module SQL
                
                     ##
                     # Generic SQL query ORDER BY token.
                     #
                     
                     class OrderBy < FluentQuery::Drivers::Shared::Tokens::SQLToken

                        ##
                        # Renders this token.
                        #

                        public
                        def render!(mode = :build)
                            
                            stack = [ ]
                            custom = [ ]
                            unknown = [ ]
                            fields = [ ]
                            distinct = false
                            
                            result = "ORDER BY "
                            processor = @_query.processor
                            _class = self.unknown_token


                            # Process subtokens
                            
                            @_subtokens.each do |token|
                                arguments = token.arguments
                                name = token.name

                                # Known select process
                                if name == :orderBy
                                    length = arguments.length
                                    close_custom = false
                                    
                                    if length > 0
                                        if (length > 1) or (arguments.first.kind_of? String)
                                            value = processor.process_formatted(arguments, mode).dup
                                            custom.push(value)

                                            # Closes fields if they are set
                                            if not fields.empty?
                                                stack << (processor.process_identifiers(fields) << ", ")
                                                fields = [ ]
                                            end
                                            
                                        elsif arguments.first.kind_of? Array
                                            fields += arguments.first
                                            close_custom = true
                                            
                                        elsif arguments.first.kind_of? Symbol
                                            fields += arguments
                                            close_custom = true

                                        end
                                        
                                        # Closes custom formulations if they are set
                                        #  and it's required.
                                        if close_custom and not custom.empty?
                                            stack << (self._close_custom(custom) << ", ")
                                            custom = [ ]
                                        end
                                    end

                                    # Closes opened native token with unknown tokens
                                    if unknown.length > 0
                                        native = _class::new(@_driver, @_query, unknown)
                                        stack << (native.render! + ", ")
                                        unknown = [ ]
                                    end

                                # Unknowns arguments pushes to the general native token
                                else
                                    unknown << token

                                    if not fields.empty?
                                        stack << processor.process_identifiers(fields)
                                        fields = [ ]
                                    elsif not custom.empty?
                                        stack << self._close_custom(custom)
                                        custom = [ ]
                                    end
                                end
                            end

                            # Closes

                                # Opened native token with unknown tokens
                                if unknown.length > 0
                                    native = _class::new(@_driver, @_query, unknown)
                                    value = native.render!
                                    unknown = [ ]

                                    if (not fields.empty?) or (not custom.empty?)
                                        value << ", "
                                    end
                                    
                                    stack << value
                                end

                                # Fields list
                                if not fields.empty?
                                    value = processor.process_identifiers(fields)

                                    if not custom.empty?
                                        value << ", "
                                    end

                                    stack << value
                                end

                                # Custom list
                                if not custom.empty?
                                    stack << self._close_custom(custom)
                                end

                            # Process stack with results
                            result << stack.join(" ")

                            return result

                        end

                        ##
                        # Closes array of custom (formatted) strings.
                        #

                        protected
                        def _close_custom(custom)
                            custom.join(", ")
                        end
                     end
                 end
             end
         end
     end
 end

