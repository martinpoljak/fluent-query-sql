# encoding: utf-8
require "set"
require "abstract"
require "hash-utils/module"   # >= 0.13.0
require "hash-utils/string"   # >= 0.15.0
require "hash-utils/array"    # >= 0.17.0
require "hash-utils/object"   # >= 0.8.0

require "fluent-query/driver"
require "fluent-query/queries/sql"
require "fluent-query/drivers/shared/tokens/sql"

module FluentQuery
    module Drivers
        
        ##
        # Represents abstract SQL driver.
        #
         
        class SQL < FluentQuery::Driver

            ##
            # Contains relevant methods index for this driver.
            #
                
            RELEVANT = Set::new [:select, :insert, :update, :delete, :truncate, :set, :begin, :commit, :union, :rollback]

            ##
            # Contains ordering for typicall queries.
            #
            
            ORDERING = {
                :select => [
                    :select, :from, :join, :groupBy, :having, :where, :orderBy, :limit, :offset
                ],
                :insert => [
                    :insert, :values
                ],
                :update => [
                    :update, :set, :where
                ],
                :delete => [
                    :delete, :where
                ],
                :truncate => [
                    :truncate, :table, :cascade
                ],
                :set => [
                    :set
                ],
                :union => [
                    :union
                ]
            }

            ##
            # Contains operators list.
            #
            # Operators are defined as tokens whose multiple parameters in Array
            # are appropriate to join by itself.
            #
            
            OPERATORS = {
                :and => "AND",
                :or => "OR"
            }

            ##
            # Indicates, appropriate token should be present by one real token, but more input tokens.
            #

            AGREGATE = Set::new [:where, :orderBy, :select, :groupBy, :having]

            ##
            # Indicates token aliases.
            #

            ALIASES = {
                :leftJoin => :join,
                :rightJoin => :join,
                :fullJoin => :join
            }

            ##
            # Contains relevant methods index for this driver.
            #

            @relevant

            ##
            # Contains ordering for typicall queries.
            #

            @ordering

            ##
            # Contains operators list.
            #
            # Operators are defined as tokens whose multiple parameters in Array
            # are appropriate to join by itself.
            #

            @operators
            
            ##
            # Indicates, appropriate token should be present by one real token, but more input tokens.
            #

            @agregate

            ##
            # Indicates token aliases.
            #

            @aliases

            ##
            # Holds native connection settings.
            #

            @_nconnection_settings

            ##
            # Holds native connection.
            # (internal cache)
            #

            @_nconnection

            ##
            # Indicates tokens which has been already required.
            #
            
            @_tokens_required
                            
            ##
            # Holds internal token struct.
            #
            
            TOKEN = Struct::new(:name, :subtokens)

            ##
            # Initializes driver.
            #

            public
            def initialize(connection)
                if self.instance_of? SQL
                    not_implemented
                end

                super(connection)

                @relevant = SQL::RELEVANT
                @ordering = SQL::ORDERING
                @operators = SQL::OPERATORS
                @aliases = SQL::ALIASES
                @agregate = SQL::AGREGATE

                @_tokens_required = { }
            end
            
            ####
            
            ##
            # Indicates, method is relevant for the driver.
            #

            public
            def relevant_method?(name)
                name.in? @relevant
            end
            
            ##
            # Indicates token is known.
            #

            public
            def known_token?(group, token_name, cache)

                # Checks
                if @ordering.has_key? group
                    # Creates index
                    if not cache.has_key? group  
                        @ordering[group].each do |item|
                            cache[group][item] = true
                        end
                    end
                
                    result = cache[group].has_key? token_name.to_sym
                else
                    result = false
                end

                return result
                
            end    

            ##
            # Builds given query.
            #

            public
            def build_query(query, mode = :build)
                self._build_query(query, FluentQuery::Drivers::Shared::Tokens::SQLToken, mode)
            end

            ##
            # Indicates, token is operator.
            #

            public
            def operator_token?(token_name)
                @operators.has_key? token_name
            end

            ##
            # Returns operator string according to operator symbol.
            #

            public
            def quote_operator(operator)
                @operators[operator]
            end

            ##
            # Returns correct equality operator for given datatype.
            #
            # Must handle three modes:
            #   * "assigning" which classicaly keeps for example the '=' operator,
            #   * "comparing" which sets for example 'IS' operator for booleans,
            #

            public
            def quote_equality(datatype, mode = :comparing)                    
                if not datatype.kind_of? Class
                    datatype = datatype.class
                end
                
                if mode == :comparing
                    if (datatype == TrueClass) or (datatype == FalseClass) or (datatype == NilClass)
                        result = "IS"
                    elsif datatype == Array
                        result = "IN"
                    else
                        result = "="
                    end
                else
                    result = "="
                end

                return result
            end
            
            ##
            # Indicates which query subclass to use.
            #
 
            public
            def query_class
                FluentQuery::Queries::SQL
            end
             
            ##
            # Builds given query.
            #

            protected
            def _build_query(query, inittoken_class, mode = :build)
            
                stack = query.stack
                token_struct = self.class::TOKEN
                tokens = [ ]

                last_known = nil
                result = ""


                # Gets type of the query (select/update/insert/delete)

                type = query.type
                
                # Sorts according to known token type
                #
                # Unknown tokens are joined to the same subarray as preceded
                # known tokens.
                #
                
                stack.each do |item|
                    name = item.name
                    
                    # If token is alias, renames it
                    
                    _alias = @aliases[name]

                    if not _alias.nil?
                        #_alias = _alias
                        
                        item.alias = _alias
                        name = _alias
                    end
                    
                    ##
                    
                    if self.known_token? type, name
                        
                        if not last_known.nil?
                            last_name = last_known.first.name
                            tokens.push(token_struct::new(last_name, last_known))
                        end

                        last_known = [item]
                        
                    elsif not last_known.array?
                        tokens.push(token_struct::new(:__init__, [item]))
                        
                    else
                        last_known << item
                        
                    end
                end

                if last_known
                    last_name = last_known.first.name
                    tokens.push(token_struct::new(last_name, last_known))
                end

                # Generates native tokens

                native_tokens = [ ]
                init_tokens = nil
                agregates = Hash::new { |hash, key| hash[key] = [ ] }

                tokens.each do |data|
                    name = data.name
                    subtokens = data.subtokens
                    
                    if (name == :__init__) and (not subtokens.nil?)
                        init_tokens = [inittoken_class::new(self, query, subtokens)]
                    
                    elsif name.in? @agregate
                        agregates[name] << data
                        
                    elsif self.known_token? type, name
                        token_data = self._generate_token(name, query, subtokens)
                        native_tokens << token_data
                    
                    end
                    
                end

                # Process agregate tokens

                agregates.each_value do |tokens|
                    first = tokens.shift

                    tokens.each do |token|
                        first.subtokens += token.subtokens
                    end

                    token_data = self._generate_token(first.name, query, first.subtokens)
                    native_tokens << token_data
                end

                # Renders in ordered way
                if not init_tokens.nil?
                    init_tokens.each do |token|
                        result << token.render!(mode) << " "
                    end
                end        

                if @ordering.has_key? type
                    @ordering[type].each do |item|
                        native_tokens.each do |data|
                            if data.name == item
                                data.subtokens.each do |token|
                                    result << token.render!(mode) << " "
                                end
                            end
                        end
                    end
                end

                result.strip!
                return result
            end

            ##
            # Generates token from token data.
            #

            protected
            def _generate_token(name, query, subtokens)
                classobject = self._require_token(name)
                tokens = [classobject::new(self, query, subtokens)]
                
                ###

                return self.class::TOKEN::new(name, tokens)
            end
                            
            ##
            # Requires token class.
            #
            
            protected
            def _require_token(name)

                if not @_tokens_required.has_key? name
                    begin
                        module_name = name.to_s.ucfirst
                        classname = "FluentQuery::Drivers::Shared::Tokens::SQL::" << module_name
                        classfile = classname.downcase
                        classfile.strtr!("::" => "/", "fluentquery" => "fluent-query")

                        Kernel.require(classfile)
                        classobject = FluentQuery::Drivers::Shared::Tokens::SQL.get_module(module_name)
                    rescue NameError, LoadError 
                        classobject = FluentQuery::Drivers::Shared::Tokens::SQLToken
                    end
                    
                     @_tokens_required[name] = classobject
                end 
                
                return @_tokens_required[name]
            end




            ##### QUOTING

            ##
            # Quotes string.
            #

            public
            def quote_string(string)
                string = string.gsub("'", "''")
                string.gsub!("\\", "\\\\\\\\")
                
                return "'" + string + "'"
            end

            ##
            # Quotes integer.
            #

            public
            def quote_integer(integer)
                integer.to_s
            end

            ##
            # Quotes float.
            #

            public
            def quote_float(float)
                float.to_s
            end

            ##
            # Quotes field by field quoting.
            #

            public
            def quote_identifier(field)
                '"' + field.to_s.gsub(".", '"."') + '"'
            end

            ##
            # Creates system-dependent NULL.
            #

            public
            def null
                "NULL"
            end

            ##
            # Quotes system-dependent boolean value.
            #

            public
            def quote_boolean(boolean)
                boolean ? 1 : 0
            end

            ##
            # Quotes system-dependent date value.
            #

            public
            def quote_date_time(date)
                self.quote_string(date.to_s)
            end

            ##
            # Quotes system-dependent subquery.
            #

            public
            def quote_subquery(subquery)
                "(" + subquery + ")"
            end


            ##### EXECUTING

            ##
            # Opens the connection.
            #
            # It's lazy, so it will open connection before first request through
            # {@link native_connection()} method.
            #

            public
            def open_connection(settings)
                not_implemented
            end
                                
            ##
            # Checks query conditionally. It's called after first token
            # of the query.
            #
            # @see #execute_conditionally
            # @since 0.9.2
            #
            
            public
            def check_conditionally(query, sym, *args, &block)
                self.execute_conditionally(query, sym, *args, &block)
            end
            
            ##
            # Executes query conditionally.
            #
            # If query isn't suitable for executing, returns it. In otherwise
            # returns +nil+, result or number of changed rows.
            #

            public
            def execute_conditionally(query, sym, *args, &block)
                case query.type
                    when :insert
                        if (args.first.symbol?) and (args.second.hash?)
                            result = query.do!
                        end
                    when :begin
                        if args.empty?
                            result = query.execute!
                        end
                    when :truncate
                        if args.first.symbol?
                            result = query.execute!
                        end
                    when :commit, :rollback
                        result = query.execute!
                    else
                        result = nil
                end
                
                return result
            end
            
        end
    end
end

