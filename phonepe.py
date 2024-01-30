import os
import json
import pandas as pd
import mysql.connector
import plotly.express as px
from sqlalchemy import create_engine, exc
import geopandas as gpd
import matplotlib.pyplot as plt
import streamlit as st



with st.sidebar:
    st.title(":orange[Phonepe Pulse Data Visualization and Exploration]")
    st.header("Notes")
    st.caption("If graph is empty there is no Data available from client")
    st.caption("If pie chart is empty there is no Data availabe in the entre year and quarter")
    st.caption("Data Management using SQL")


#sql connection 
import mysql.connector

# Replace these values with your own MySQL server details
host = "localhost"
user = "root"
password = "Pass@12345678"
database = "phonepe_database"  # Replace with your actual database name

# Establish a connection to the MySQL server with the specified database
conn = mysql.connector.connect(host=host, user=user, password=password, database=database)

# Create a cursor object to execute SQL queries
cursor = conn.cursor()

question = st.selectbox("select your question",("1. List of the Total Transaction_count that happen state-wise",
                                                "2. A list of the highest brand users that happen state-wise",
                                                "3. A list of the highest total transaction amounts that happen state-wise",
                                                "4. A list of the top 10 highest user counts that happen state-wise",
                                                "5. A list of the insurance total transaction counts that happen state-wise",
                                                "6. A list of the insurance total ammount that happen state-wise",
                                                "7. A list of the top 20 highest insurance count that happen districts-wise",
                                                "8. A list of the top 10 highest Total_insurance_Amount that happen districts-wise",
                                                "9. A list of the top 10 highest Total_Transaction_Amount that happen districts-wise",
                                                "10. A list of the top 10 highest user counts in order of state"))


option_1 = st.selectbox('Select the year',(2014,2015,2016,2017,2018,2019,2020,2021,2022))
st.write('You selected a year:', option_1)
option_2 = st.selectbox('Select the Quarter',(1, 2, 3, 4))
st.write('You selected a Quarter:', option_2)

if question == "1. List of the Total Transaction_count that happen state-wise":

    # Get input from the user
    year = option_1
    quarter = option_2

    # Construct the SQL query with parameterized values
    sql_query = """SELECT States, SUM(Transaction_count) AS TotalTransactionCount 
                FROM Aggregated_Transaction 
                WHERE Years = %s AND Quarter = %s 
                GROUP BY States order by TotalTransactionCount desc;"""

    # Execute the query with the provided parameters
    cursor.execute(sql_query, (year, quarter))
    t1 = cursor.fetchall()  
    df1 = pd.DataFrame(t1, columns=["States", "TotalTransactionCount"])

    # Assuming df1 is the DataFrame obtained from your SQL query
    fig = px.bar(df1, x='States', y='TotalTransactionCount', color = 'TotalTransactionCount',
                title=f'Transaction Counts by State for Year: {year} - Quarter: {quarter}',text_auto = True)
    fig.update_traces(textfont_size = 14, textangle = 0, textposition = "outside")

    # Show the plot
    #fig.show()
    st.plotly_chart(fig)

    # Close the cursor and connection
    cursor.close()
    conn.close()

elif question == "2. A list of the highest brand users that happen state-wise":

    # Get input from the user
    year = option_1
    quarter = option_2

    # Construct the SQL query with parameterized values
    sql_query_2 = """
        SELECT
            States,
            Years,
            quarter,
            Brands,
            User_count,
            User_percentage
        FROM (
            SELECT
                States,
                Years,
                quarter,
                Brands,
                User_count,
                User_percentage,
                RANK() OVER (PARTITION BY States, Years, quarter ORDER BY User_count DESC) AS brand_rank
            FROM aggregated_user_table
            WHERE Years = %s AND quarter = %s  # Add user input as parameters
        ) ranked_data
        WHERE brand_rank = 1;
    """

    # Execute the query with the provided parameters
    cursor.execute(sql_query_2, (year, quarter))
    t2 = cursor.fetchall()

    # Assuming df2 is the DataFrame obtained from your SQL query
    df2 = pd.DataFrame(t2, columns=["States", "Years", "Quarter", "Brands", "User_count", "User_percentage"])

    # Assuming df2 is the DataFrame obtained from your SQL query
    fig = px.bar(df2, x='States', y='User_count', color='Brands',
                title=f'Highest brand users Counts by State for Year: {year} - Quarter: {quarter}')

    # Set the category order for "States" in descending order based on User_count
    fig.update_layout(xaxis_categoryorder='total descending')

    # Show the plot
    #fig.show()
    st.plotly_chart(fig)

    # Close the cursor and connection
    cursor.close()
    conn.close()

elif question == "3. A list of the highest total transaction amounts that happen state-wise":

    # Get input from the user
    year = option_1
    quarter = option_2

    # Construct the SQL query with parameterized values
    sql_query_3 = """
        SELECT States, SUM(Transaction_amount) AS Total_Transaction_Amount
        FROM Aggregated_Transaction
        WHERE Years = %s AND Quarter = %s
        GROUP BY States order by Total_Transaction_Amount desc;
    """

    # Execute the query with the provided parameters
    cursor.execute(sql_query_3, (year, quarter))
    t3 = cursor.fetchall()  
    df3 = pd.DataFrame(t3, columns=["States", "Total_Transaction_Amount"])

    # Assuming df1 is the DataFrame obtained from your SQL query
    fig = px.bar(df3, x='States', y='Total_Transaction_Amount', color = 'Total_Transaction_Amount',
                title=f'Highest total transaction amounts by State for Year: {year} - Quarter: {quarter}',text_auto = True)
    fig.update_traces(textfont_size = 14, textangle = 0, textposition = "outside")
    st.plotly_chart(fig)

    # Close the cursor and connection
    cursor.close()
    conn.close()

elif question == "4. A list of the top 10 highest user counts that happen state-wise":

    # Get input from the user
    year = option_1
    quarter = option_2

    # Construct the SQL query with parameterized values
    sql_query_4 = """
        SELECT States, SUM(User_count) AS TotalUsers
        FROM aggregated_user_table
        WHERE Years = %s AND Quarter = %s
        GROUP BY States
        ORDER BY TotalUsers DESC
        LIMIT 10; -- Adjust the limit based on the number of top states you want to display
    """

    # Execute the query with the provided parameters
    cursor.execute(sql_query_4, (year, quarter))
    t4 = cursor.fetchall()

    # Assuming df2 is the DataFrame obtained from your SQL query
    df4 = pd.DataFrame(t4, columns=["States", "TotalUsers"])

    # Assuming df2 is the DataFrame obtained from your SQL query
    fig = px.bar(df4, x='States', y='TotalUsers', color='TotalUsers',
                title=f'Top 10 highest user Counts by State for Year: {year} - Quarter: {quarter}')
    st.plotly_chart(fig)

    # Close the cursor and connection
    cursor.close()
    conn.close()

elif question == "5. A list of the insurance total transaction counts that happen state-wise":

    # Get input from the user
    year = option_1
    quarter = option_2

    # Construct the SQL query to get the total transaction count state-wise
    sql_query_5 = """
        SELECT States, SUM(Transaction_count) AS total_transaction_count
        FROM Aggregated_Insurance_Table
        WHERE Years = %s AND Quarter = %s
        GROUP BY States ORDER BY total_transaction_count DESC;
    """

    # Execute the query with the provided parameters
    cursor.execute(sql_query_5, (year, quarter))
    result = cursor.fetchall()

    # Assuming df5 is the DataFrame obtained from your SQL query
    df5 = pd.DataFrame(result, columns=["States", "total_transaction_count"])

    # Assuming df5 is the DataFrame obtained from your SQL query
    fig = px.bar(df5, x='States', y='total_transaction_count', color='total_transaction_count',
                title=f'Insurance Total Transaction Counts by State for Year: {year} - Quarter: {quarter}')
    st.plotly_chart(fig)

    # Close the cursor and connection
    cursor.close()
    conn.close()

elif question == "6. A list of the insurance total ammount that happen state-wise":

    # Get input from the user
    year = option_1
    quarter = option_2

    # Construct the SQL query to get the total transaction count state-wise
    sql_query_6 = """
        SELECT States, SUM(Transaction_amount) AS TotalAmount
        FROM Aggregated_Insurance_Table
        WHERE Years = %s AND Quarter = %s
        GROUP BY States ORDER BY TotalAmount DESC;
    """

    # Execute the query with the provided parameters
    cursor.execute(sql_query_6, (year, quarter))
    result = cursor.fetchall()

    # Assuming df5 is the DataFrame obtained from your SQL query
    df6 = pd.DataFrame(result, columns=["States", "Total Amount"])

    # Assuming df6 is the DataFrame obtained from your SQL query
    fig = px.bar(df6, x='States', y='Total Amount', color='Total Amount',
                title=f'Insurance Total Transaction Counts by State for Year: {year} - Quarter: {quarter}')
    st.plotly_chart(fig)

    # Close the cursor and connection
    cursor.close()
    conn.close()

elif question == "7. A list of the top 20 highest insurance count that happen districts-wise":

    # Get input from the user
    year = option_1
    quarter = option_2

    # Construct the SQL query to get the total transaction count state-wise
    sql_query_7 = """
        SELECT Districts_name, sum(Total_insurance_count) AS highest_insurance_count
        FROM Map_Insurance_Table
        WHERE Years = %s AND Quarter = %s
        GROUP BY Districts_name ORDER BY highest_insurance_count DESC limit 20;
    """

    # Execute the query with the provided parameters
    cursor.execute(sql_query_7, (year, quarter))
    result = cursor.fetchall()

    # Assuming df5 is the DataFrame obtained from your SQL query
    df7 = pd.DataFrame(result, columns=["Districts name", "Highest Insurance Count"])

    # Assuming df6 is the DataFrame obtained from your SQL query
    fig = px.bar(df7, x='Districts name', y='Highest Insurance Count', color='Highest Insurance Count',
                title=f'Top 20 highest insurance count by districts for Year: {year} - Quarter: {quarter}')
    st.plotly_chart(fig)

    # Close the cursor and connection
    cursor.close()
    conn.close()

elif question == "8. A list of the top 10 highest Total_insurance_Amount that happen districts-wise":

    # Get input from the user
    year = option_1
    quarter = option_2

    # Construct the SQL query to get the top 10 highest Total_insurance_Amount districts-wise
    sql_query_8 = """
    SELECT
        Districts_name,
        SUM(Total_insurance_Amount) AS TotalAmount
    FROM
        Top_Insurance_Table
    WHERE Years = %s AND Quarter = %s
    GROUP BY
        Districts_name
    ORDER BY
        TotalAmount DESC
    LIMIT 10;
    """

    # Execute the query with the provided parameters
    cursor.execute(sql_query_8, (year, quarter))
    result = cursor.fetchall()

    # Assuming df8 is the DataFrame obtained from your SQL query
    df8 = pd.DataFrame(result, columns=["Districts name", "Highest Insurance Amount"])

    # Display via pie chart
    fig = px.pie(df8, values="Highest Insurance Amount", names="Districts name",
                title=f'Top 10 Highest Insurance Amount by Districts for Year: {year} - Quarter: {quarter}')
    st.plotly_chart(fig)

    # Close the cursor and connection
    cursor.close()
    conn.close()

elif question == "9. A list of the top 10 highest Total_Transaction_Amount that happen districts-wise":

    # Get input from the user
    year = option_1
    quarter = option_2

    # Construct the SQL query to get the top 10 highest Total_insurance_Amount districts-wise
    sql_query_9 = """
    SELECT
        Districts_name,
        SUM(Transaction_Amount) AS TransactionAmount
    FROM
        Top_Transaction_Table
    WHERE Years = %s AND Quarter = %s
    GROUP BY
        Districts_name
    ORDER BY
        TransactionAmount DESC
    LIMIT 10;
    """

    # Execute the query with the provided parameters
    cursor.execute(sql_query_9, (year, quarter))
    result = cursor.fetchall()

    # Assuming df8 is the DataFrame obtained from your SQL query
    df9 = pd.DataFrame(result, columns=["Districts name", "Highest Transaction Amount"])

    # Display via pie chart
    fig = px.pie(df9, values="Highest Transaction Amount", names="Districts name",
                title=f'Top 10 highest Total Transaction Amount by Districts for Year: {year} - Quarter: {quarter}')
    st.plotly_chart(fig)

    # Close the cursor and connection
    cursor.close()
    conn.close()

elif question == "10. A list of the top 10 highest user counts in order of state":

    # Get input from the user
    year = option_1
    quarter = option_2

    # Construct the SQL query to get the top 10 highest Total_insurance_Amount districts-wise
    sql_query_10 = """
    SELECT
        States,
        SUM(RegisteredUsers_count) AS RegisteredUserscount
    FROM
        Top_User_Table
    WHERE Years = %s AND Quarter = %s
    GROUP BY
        States
    ORDER BY
        RegisteredUserscount DESC
    LIMIT 10;
    """
    
    # Execute the query with the provided parameters
    cursor.execute(sql_query_10, (year, quarter))
    result = cursor.fetchall()

    # Assuming df8 is the DataFrame obtained from your SQL query
    df10 = pd.DataFrame(result, columns=["States", "RegisteredUserscount"])
    print(df10)

    # Display via pie chart
    fig = px.pie(df10, values="RegisteredUserscount", names="States",
                title=f'Top 10 highest user counts by states for Year: {year} - Quarter: {quarter}')
    st.plotly_chart(fig)

    # Close the cursor and connection
    cursor.close()
    conn.close()