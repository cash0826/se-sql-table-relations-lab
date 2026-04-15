# STEP 0

# SQL Library and Pandas Library
import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect('data.sqlite')

pd.read_sql("""SELECT * FROM sqlite_master""", conn)

# STEP 1
q1 = """
SELECT 
  firstName, 
  jobtitle
FROM employees
JOIN offices
  USING(officeCode)
WHERE city = "Boston";
"""

df_boston = pd.read_sql(q1, conn)
# print(df_boston)

# STEP 2
q2 = """
SELECT
  o.officeCode,
  o.city,
  COUNT(e.employeeNumber) AS n_employees
FROM offices AS o
LEFT JOIN employees AS e
  USING(officeCode)
GROUP BY officeCode
HAVING n_employees = 0
;
"""
df_zero_emp = pd.read_sql(q2, conn)
# print(df_zero_emp)

# STEP 3
q3 = """
SELECT 
  firstName,
  lastName,
  city,
  state
FROM employees
LEFT JOIN offices
  USING(officeCode)
ORDER BY firstName, lastName;
"""

df_employee = pd.read_sql(q3, conn)
# print(df_employee)

# STEP 4
q4 = """
SELECT
  contactFirstName,
  contactLastName,
  phone,
  salesRepEmployeeNumber
FROM customers
LEFT JOIN orders
  USING(customerNumber)
GROUP BY contactFirstName, contactLastName
HAVING COUNT(orderNumber) = 0
ORDER BY contactLastName
"""
df_contacts = pd.read_sql(q4, conn)
# print(df_contacts.shape)

# STEP 5
q5 = """
SELECT
  contactFirstName,
  contactLastName,
  amount,
  paymentDate
FROM customers
JOIN payments
  USING(customerNumber)
ORDER BY CAST(amount AS INTEGER) DESC;
"""

df_payment = pd.read_sql(q5, conn)
# print(df_payment)

# STEP 6
# Return the employee number, first name, last name, and number of customers for employees whose customers have an average credit limit over 90k
# Sort by # of customers from high to low
q6 = """
SELECT
  employeeNumber,
  firstName,
  lastName,
  COUNT(customerNumber) AS n_customers
FROM employees AS e
LEFT JOIN customers AS c
  ON e.employeeNumber = c.salesRepEmployeeNumber
GROUP BY employeeNumber
  HAVING AVG(creditLimit) > 90000
ORDER BY n_customers DESC;
"""

df_credit = pd.read_sql(q6, conn)
# print(df_credit)

# STEP 7
# Return the product name and count the number of orders for each product as a column named numorders
# Also return a new column, totalunits, that sums up the total quantity of product sold (use the quantityOrdered column).
# Sort the results by the totalunits column, highest to lowest, to showcase the top-selling products
q7 = """
SELECT 
  productName,
  COUNT(od.orderNumber) AS numorders,
  SUM(quantityOrdered) as totalunits
FROM products AS p
JOIN orderDetails AS od
  USING(productCode)
JOIN orders AS o
  USING(orderNumber)
GROUP BY productCode
ORDER BY totalunits DESC;
"""
df_product_sold = pd.read_sql(q7, conn)
# print(df_product_sold)

# STEP 8
q8 = """
SELECT 
  productName,
  productCode,
  COUNT(DISTINCT c.customerNumber) AS numpurchasers
FROM products
JOIN orderdetails AS od
  USING(productCode)
JOIN orders AS o
  USING(orderNumber)
JOIN customers AS c
  USING(customerNumber)
GROUP BY productCode
ORDER BY numpurchasers DESC
"""

df_total_customers = pd.read_sql(q8, conn)
# print(df_total_customers)

# STEP 9
q9 = """
SELECT
  COUNT(c.customerNumber) as n_customers,
  o.officeCode AS officeCode,
  o.city AS city
FROM offices AS o
LEFT JOIN employees AS e
  USING(officeCode)
JOIN customers AS c
  ON e.employeeNumber = c.salesRepEmployeeNumber
GROUP BY officeCode
"""

df_customers = pd.read_sql(q9, conn)
# print(df_customers)

# STEP 10
# Employees who sold products that have been ordered by fewer than 20 customers
q10 = """
SELECT
  DISTINCT employeeNumber,
  firstName, 
  lastName,
  o.city AS city,
  o.officeCode AS officeCode
FROM employees AS e
JOIN offices AS o
  USING(officeCode)
JOIN customers AS c
  ON e.employeeNumber = c.salesRepEmployeeNumber
JOIN orders
  USING(customerNumber)
JOIN orderDetails
  USING(orderNumber)
WHERE productCode IN(
  SELECT
    productCode
  FROM products
  JOIN orderdetails
    USING(productCode)
  JOIN orders
    USING(orderNumber)
  GROUP BY productCode
    HAVING COUNT(DISTINCT customerNumber) < 20
)
ORDER BY lastName
"""

# subquery: find all the products that have been ordered by 19 or fewer customers

sq = """
SELECT
  productCode,
  COUNT(DISTINCT customerNumber) AS n_purchasers
FROM products AS p
LEFT JOIN orderdetails AS od
  USING(productCode)
LEFT JOIN orders AS o
  USING(orderNumber)
GROUP BY productCode
  HAVING COUNT(DISTINCT customerNumber) < 20
"""

# sq_df = pd.read_sql(sq, conn)
# print(sq_df)

df_under_20 = pd.read_sql(q10, conn)
print(df_under_20)

conn.close()