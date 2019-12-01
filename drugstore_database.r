#creating the connection to the server
conn_url = 'postgresql://postgres:pwd4APAN5310@localhost::5432/abc_pharmacy'

engine = create_engine(conn_url)

connection = engine.connect()

#create the tables on the database
stmt = """
    CREATE TABLE patients (
        patient_id   SERIAL,
        first_name    varchar(50) NOT NULL,
        last_name     varchar(50) NOT NULL,
        email         varchar(50) NOT NULL UNIQUE,
        PRIMARY KEY (patient_id)
    );
    
    CREATE TABLE phones (
        phone_id      SERIAL,
		patient_id	  integer,
        phone_type    varchar(4) NOT NULL,
		phone_number  varchar(12) NOT NULL,
        PRIMARY KEY (phone_id, patient_id),
		FOREIGN KEY (patient_id) REFERENCES patients(patient_id),
		CHECK(phone_type IN ('cell','home')
    );
    
    CREATE TABLE drugs (
        drug_id      	SERIAL,
		drug_comapany	varchar(100) NOT NULL,
        drug_name		varchar(200) NOT NULL,
        PRIMARY KEY (drug_id)
    );
    
    CREATE TABLE purchases (
        purchase_id      	SERIAL,
        patient_id      	integer,
		purchase_timestamp	timestamp NOT NULL,
        PRIMARY KEY (purchase_id),
        FOREIGN KEY (patient_id) REFERENCES patients(patient_id)
    );
    
    CREATE TABLE purchase_details (
        purchase_id   		integer,
        drug_id      		integer,
        unit_price   		numeric(6,2) NOT NULL,
        quantity  			integer NOT NULL,
        PRIMARY KEY (purchase_id, drug_id),
        FOREIGN KEY (purchase_id) REFERENCES purchases(purchase_id),
        FOREIGN KEY (drug_id) REFERENCES drugs(drug_id)
    );
"""

connection.execute(stmt)

#Begin ETL - Extract, Transform, Load process
library(tidyverse)

#path will change depending on where file is located
data = read.csv('APAN5310f19_HW6_DATA.csv') 


# add patient id and purchase id to main table for traceability
data = data %>% 
    mutate(patient_id = group_indices_(data, .dots=c("first_name", "last_name" , "email"))) 

data = data %>%
    mutate(purchase_id = group_indices_(data, .dots=c("patient_id", "purchase_timestamp")))

	
# making separate cell phone and home phone columns
data = data %>% 
  separate(cell_and_home_phones, into = c('cell_phone','home_phone'),sep = ';',extra = "merge")


#removing dollar signs from drug prices
data$price_1 = as.numeric(gsub('[$,]', '', data$price_1))
data$price_2 = as.numeric(gsub('[$,]', '', data$price_2))
  

#building the drugs table
drug_1 = data.frame('drug_name' = data$drug_name_1,
                       'drug_company' = data$drug_company_1,
                       'quantity' = data$quantity_1,
                       'unit_price' = data$price_1,
                       'purchase_id' = data$purchase_id)
drug_2 = data.frame('drug_name' = data$drug_name_2,
                       'drug_company' = data$drug_company_2,
                       'quantity' = data$quantity_2,
                       'unit_price' = data$price_2,
                       'purchase_id' = data$purchase_id)
drugs = rbind(drug_1,drug_2)


#making main table with only one drug per row
data_sub = subset(data, select = c('first_name','last_name','email','cell_phone','home_phone','purchase_timestamp','patient_id','purchase_id'))
data = merge(data_sub,drugs, by = 'purchase_id')


#getting rid of row with no drug names or blank drug names (occurs when not a second row originally) 
data = data[!((is.na(data$drug_name)) | data$drug_name ==''),]


#drug ids to main table
data = data %>% 
    mutate(drug_id = group_indices_(data, .dots=c("drug_name", "drug_company"))) 

	
	
	
#patients table and dropping duplicates 
patients = data[c('patient_id', 'first_name', 'last_name', 'email','cell_phone','home_phone')]
patients = patients[!duplicated(patients['patient_id']),]


#drugs table, dropping duplicates
drugs = data.frame('drug_id' = data$drug_id, 'name' = data$drug_name, 'company' = data$drug_company)
drugs = drugs[!duplicated(drugs[c('name','company')]),]


#purchases table, dropping duplicates
purchases = data[c('purchase_id','patient_id','purchase_timestamp')]
purchases = purchases[!duplicated(purchases[c('purchase_id')]),]


#purchase details table
purchase_details = data[c('purchase_id','drug_id','unit_price','quantity')]


#tables to PgAdmin
dbWriteTable(con, name="patients", value=patients, row.names=FALSE, append=TRUE)
dbWriteTable(con, name="drugs", value=drugs, row.names=FALSE, append=TRUE)
dbWriteTable(con, name="purchases", value=purchases, row.names=FALSE, append=TRUE)
dbWriteTable(con, name="purchase_details", value=purchase_details, row.names=FALSE, append=TRUE)



#Query the table through connection to find the customer name(s) and total purchase cost of the top 3 most expensive transactions.
stmt = "

WITH purchaseTotal as(
	SELECT purchase_id, SUM(unit_price * quantity) as total_amount,
		   RANK() OVER (ORDER BY SUM(unit_price * quantity) DESC) as rank1
 	FROM purchase_details 
	GROUP BY purchase_id
	ORDER BY rank1
)

SELECT patients.first_name as first_name, patients.last_name as last_name, pt.total_amount as total_amount
FROM purchaseTotal as pt
JOIN purchases as p
ON p.purchase_id = pt.purchase_id
JOIN patients 
ON patients.patient_id = p.patient_id
WHERE rank1 <= 3
ORDER BY rank1;

"

temp_df = dbGetQuery(con, stmt)

temp_df



# creating a histogram of drug prices
stmt = "
    SELECT unit_price
    FROM purchase_details
"

df = dbGetQuery(con, stmt)

hist(df$unit_price,
	main="Drug Price Histogram",
     xlab="Unit Prices",
     ylab = "Frequency")