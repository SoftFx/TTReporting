

#get lpexec datatable
getaggrlpexecPeriod <- function(Credsaggr, From, To){
  ConnectToDB(dbname = Credsaggr$postgre_DB, user = Credsaggr$postgre_USER, password = Credsaggr$postgre_PASSWORD, host = Credsaggr$postgre_HOST)
  setPathToSchema(Credsaggr$postgre_SCHEMA)
  querry <- paste('select * from "lpexecution" where "time" >= ', quoteString(From),' and "time" < ', quoteString(To), 'and "filled_volume" > 0 ')
  res <- GetDataFromDB(DBCON, querry)
  setDefaultSchema()
  DissconnectFromDB()
  res <- as.data.table(res)
  res[, AGGR := Credsaggr$postgre_SCHEMA]
  return(res)
}


#looooong time execution querry
# getaggrlpexecPeriod <- function(Credsaggr, From, To){
#   ConnectToDB(dbname = Credsaggr$postgre_DB, user = Credsaggr$postgre_USER, password = Credsaggr$postgre_PASSWORD, host = Credsaggr$postgre_HOST)
#   setPathToSchema(Credsaggr$postgre_SCHEMA)
#   querry <- paste('select *,
#   	                      case 
# 	                        	when "order_type" = 0 then \'Undefined\'
# 	   		                  	when "order_type" = 1 then \'SOSL\'
# 	   		                  	when "order_type" = 2 then \'Regular\'
# 	   		                  	when "order_type" = 3 then \'Limit\'
# 	   		                  	when "order_type" = 4 then \'Stop\'
# 	   		                  	when "order_type" = 5 then \'ForcedLimit\'
# 	                         end as "OrderType",
# 	                        case 
# 	                        	when "command_name" = 0 then \'OP_BUY\'
# 	   		                  	when "command_name" = 1 then \'OP_SELL\'
# 	   		                  	when "command_name" = 2 then \'OP_BUY_LIMIT\'
# 	   		                  	when "command_name" = 3 then \'OP_SELL_LIMIT\'
# 	   		                  	when "command_name" = 4 then \'OP_BUY_STOP\'
# 	   		                  	when "command_name" = 5 then \'OP_SELL_STOP\'
# 	   		                  	when "command_name" = 6 then \'OP_BALANCE\'
# 	   		                  	when "command_name" = 7 then \'OP_CREDIT\'
# 	   		                  	when "command_name" = 8 then \'OP_BUY_STOP_LIMIT\'
# 	   		                  	when "command_name" = 9 then \'OP_SELL_STOP_LIMIT\'
# 	                         end as "command"
#                   from "lpexecution" where "time" >= ', quoteString(From),' and "time" < ', quoteString(To))
#   res <- GetDataFromDB(DBCON, querry)
#   setDefaultSchema()
#   DissconnectFromDB()
#   res <- as.data.table(res)
#   res[, AGGR := Credsaggr$postgre_SCHEMA]
#   return(res)
# }