# encoding: utf-8
require "hash-utils/object"   # >= 0.17.0

module FluentQuery
    module Drivers
        module Shared
            module Tokens

                 ##
                 # PostgreSQL query native token.
                 #
                 
                 class SQLToken

                    ##
                    # Query to which is this token associated to.
                    #

                    @_query

                    ##
                    # Parent driver.
                    #

                    @_driver
                    
                    ##
                    # Original subtokens associated to native token.
                    #

                    @_subtokens
                    
                    ##
                    # Holds token transformer.
                    #
                    
                    TRANSFORMER = /([A-Z])/
                    
                    ##
                    # Initializes token.
                    #

                    public
                    def initialize(driver, query, subtokens)
                        @_query = query
                        @_driver = driver
                        @_subtokens = subtokens
                    end

                    ##
                    # Renders this token.
                    #
                    # Name of the token transforms to the SQL form. Joins arguments
                    # in Array by commas, in Hash by name = value separated by
                    # commas form or by token body itself if token is operator.
                    #
                    # Mode can be :prepare or :build. Preparing means building the 
                    # query without expanding the formatting directives.
                    #
                    
                    public
                    def render!(mode = :build)

                        result = ""
                        processor = @_query.processor

                        # Transforms all subtokens to real string SQL tokens
                        
                        @_subtokens.each do |token|

                            arguments = token.arguments
                            name = token.name
                            transformed = self._transform_token(name.to_s)
                            operator = @_driver.operator_token? name
                            
                            if operator
                                glue = transformed
                            else
                                glue = ","
                            end

                            first = arguments.first
                            
                            if first.array?
                                arguments_result = processor.process_array(first, glue)
                            elsif first.hash?
                                arguments_result = processor.process_hash(first, glue)
                            elsif first.symbol?
                                arguments_result = processor.process_identifiers(arguments)
                            else
                                arguments_result = processor.process_formatted(arguments, mode)
                            end
                            
                            result << transformed << " " << arguments_result << " "
                        end

                        return result
                    end

                    ##
                    # Transform token name to the SQL token.
                    #
                    # Upper case characters handles as separators, so for example:
                    #   'leftJoin' will be transformed to 'LEFT JOIN'
                    #

                    protected
                    def _transform_token(token_name)
                        sql_name = token_name.to_s.gsub(self.class::TRANSFORMER, ' \1')
                        sql_name.upcase!
                        
                        return sql_name
                    end
                    
                    ##
                    # Returns appropriate class for anonymous tokens.
                    #
                    
                    def unknown_token
                        FluentQuery::Drivers::Shared::Tokens::SQLToken
                    end
                                                            
                 end
             end
         end
     end
 end

