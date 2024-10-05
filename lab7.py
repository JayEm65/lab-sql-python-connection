import pandas as pd
from sqlalchemy import create_engine
import os

# Function to create a connection to the Sakila database
def create_connection():
    # Replace 'root' and 'localhost' with your database credentials
    db_user = 'root'
    db_password = os.getenv('MY_PASSWORD')  # Retrieve password from environment variable
    db_host = 'localhost'  # Use your local IP here if needed
    db_name = 'sakila'
    
    db_url = f'postgresql://{db_user}:{db_password}@{db_host}/{db_name}'
    engine = create_engine(db_url)
    return engine

# Function to retrieve rental data for a given month and year
def rentals_month(engine, month, year):
    query = f"""
    SELECT customer_id, rental_date
    FROM rental
    WHERE EXTRACT(MONTH FROM rental_date) = {month}
    AND EXTRACT(YEAR FROM rental_date) = {year};
    """
    return pd.read_sql(query, engine)

# Function to count rentals by customer for the specified month and year
def rental_count_month(df, month, year):
    rentals_column_name = f'rentals_{month}_{year}'
    rental_counts = df.groupby('customer_id').size().reset_index(name=rentals_column_name)
    return rental_counts

# Function to compare rentals between two months
def compare_rentals(df1, df2):
    combined_df = pd.merge(df1, df2, on='customer_id', how='outer')
    combined_df['difference'] = combined_df.iloc[:, 1] - combined_df.iloc[:, 2]  # Adjust column indices as needed
    return combined_df

# Main block to execute the functions
if __name__ == '__main__':
    engine = create_connection()
    
    # Retrieve rental data for May and June
    may_data = rentals_month(engine, 5, 2005)
    june_data = rentals_month(engine, 6, 2005)

    # Count rentals for May and June
    may_counts = rental_count_month(may_data, 5, 2005)
    june_counts = rental_count_month(june_data, 6, 2005)

    # Compare rentals between May and June
    comparison = compare_rentals(may_counts, june_counts)

    # Display the comparison result
    print(comparison)

    # Export the comparison result to a CSV file
    comparison.to_csv('customer_rental_comparison.csv', index=False)
