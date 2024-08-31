import os
import psycopg2
import pandas as pd
import numpy as np
from psycopg2 import sql

# Database connection parameters
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")  # Default to 5432 if not set

# SQL commands to create the tables
create_tables_commands = [
    """
    CREATE TABLE articles (
      id SERIAL PRIMARY KEY,
      source_name VARCHAR,
      author VARCHAR,
      title VARCHAR,
      description VARCHAR,
      url VARCHAR,
      url_to_image VARCHAR,
      published_at TIMESTAMP,
      content VARCHAR,
      category VARCHAR,
      article VARCHAR,
      title_sentiment VARCHAR,
      domain_id INTEGER
    );
    """,
    """
    CREATE TABLE domains (
      id SERIAL PRIMARY KEY,
      domain_name VARCHAR,
      domain_locations_id INTEGER
    );
    """,
    """
    CREATE TABLE domain_locations (
      id SERIAL PRIMARY KEY,
      location VARCHAR,
      country VARCHAR
    );
    """,
    """
    CREATE TABLE traffic_data (
      id SERIAL PRIMARY KEY,
      global_rank INTEGER,
      tld_rank INTEGER,
      tld VARCHAR,
      ref_subnets INTEGER,
      ref_ips INTEGER,
      idn_domain VARCHAR,
      idn_tld VARCHAR,
      prev_global_rank INTEGER,
      prev_tld_rank INTEGER,
      prev_ref_subnets INTEGER,
      prev_ref_ips INTEGER,
      domain_id INTEGER
    );
    """,
    """
    ALTER TABLE domains ADD FOREIGN KEY (domain_locations_id) REFERENCES domain_locations (id);
    """,
    """
    ALTER TABLE traffic_data ADD FOREIGN KEY (domain_id) REFERENCES domains (id);
    """,
    """
    ALTER TABLE articles ADD FOREIGN KEY (domain_id) REFERENCES domains (id);
    """
]

# Function to create tables in the PostgreSQL database
def create_database():
    try:
        # Connect to PostgreSQL server
        conn = psycopg2.connect(
            dbname=DB_NAME, 
            user=DB_USER, 
            password=DB_PASSWORD, 
            host=DB_HOST, 
            port=DB_PORT
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Create tables
        for command in create_tables_commands:
            cursor.execute(command)

        print("Tables created successfully!")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
    finally:
        if conn is not None:
            cursor.close()
            conn.close()

# Function to insert or get domain ID
def get_or_insert_domain(cursor, domain_name):
    cursor.execute(
        """
        SELECT id FROM domains WHERE domain_name = %s;
        """,
        (domain_name,)
    )
    domain_id = cursor.fetchone()
    
    if not domain_id:
        cursor.execute(
            """
            INSERT INTO domains (domain_name, domain_locations_id)
            VALUES (%s, NULL)
            RETURNING id;
            """,
            (domain_name,)
        )
        domain_id = cursor.fetchone()[0]
    else:
        domain_id = domain_id[0]
    
    return domain_id

# Function to insert data into the 'domain_locations' and 'domains' tables from a DataFrame
def insert_domain_locations(df):
    location_ids = {}
    
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cursor = conn.cursor()
        
        for _, row in df.iterrows():
            # Insert or update domain location
            cursor.execute(
                """
                INSERT INTO domain_locations (location, country)
                VALUES (%s, %s)
                RETURNING id;
                """,
                (row['location'], row['Country'])
            )
            location_id = cursor.fetchone()[0]
            location_ids[row['location']] = location_id

        conn.commit()
        print("Domain locations data inserted/updated successfully!")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
    finally:
        if conn is not None:
            cursor.close()
            conn.close()
    
    return location_ids

#Function to insert data into the 'domain' table
def insert_domains(df):
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cursor = conn.cursor()

        for _, row in df.iterrows():
            # Fetch domain_locations_id from the domain_locations table
            cursor.execute(
                """
                SELECT id FROM domain_locations
                WHERE location = %s AND country = %s;
                """,
                (row['location'], row['Country'])
            )
            location_id = cursor.fetchone()

            if location_id:
                location_id = location_id[0]
                print(location_id)
                # Insert into domains
                cursor.execute(
                    """
                    INSERT INTO domains (domain_name, domain_locations_id)
                    VALUES (%s, %s)
                    """,
                    (row['SourceCommonName'], location_id)
                )
        
        conn.commit()
        print("Domains data inserted successfully!")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
    finally:
        if conn is not None:
            cursor.close()
            conn.close()

# Function to insert data into the 'traffic_data' table from a DataFrame
def insert_traffic_data(df):
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cursor = conn.cursor()

        for _, row in df.iterrows():
            domain_name = row['Domain']
            
            # Check if the domain exists and get its ID, or insert it if not
            cursor.execute(
                """
                SELECT id FROM domains WHERE domain_name = %s;
                """,
                (domain_name,)
            )
            domain_id = cursor.fetchone()
            
            if domain_id:
                domain_id = domain_id[0]
            else:
                # Insert new domain if it doesn't exist
                cursor.execute(
                    """
                    INSERT INTO domains (domain_name)
                    VALUES (%s)
                    RETURNING id;
                    """,
                    (domain_name,)
                )
                domain_id = cursor.fetchone()[0]
            print(domain_id)
            # Insert traffic data
            cursor.execute(
                """
                INSERT INTO traffic_data (global_rank, tld_rank, tld, ref_subnets, ref_ips, idn_domain, idn_tld, prev_global_rank, prev_tld_rank, prev_ref_subnets, prev_ref_ips, domain_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """,
                (
                    row['GlobalRank'], row['TldRank'], row['TLD'], row['RefSubNets'], row['RefIPs'], 
                    row['IDN_Domain'], row['IDN_TLD'], row['PrevGlobalRank'], row['PrevTldRank'], 
                    row['PrevRefSubNets'], row['PrevRefIPs'], domain_id
                )
            )

        conn.commit()
        print("Traffic data inserted successfully!")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
    finally:
        if conn is not None:
            cursor.close()
            conn.close()

# Function to insert data into the 'articles' table and handle domains
def insert_articles(df):
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cursor = conn.cursor()

        for _, row in df.iterrows():
            # Check if the domain exists in the domains table
            cursor.execute(
                """
                SELECT id FROM domains WHERE domain_name = %s;
                """,
                (row['domain'],)
            )
            result = cursor.fetchone()

            if result:
                domain_id = result[0]
                print(f"Domain ID found for domain: {row['domain']}")
            else:
                # Insert the new domain into the domains table
                cursor.execute(
                    """
                    INSERT INTO domains (domain_name)
                    VALUES (%s)
                    RETURNING id;
                    """,
                    (row['domain'],)
                )
                domain_id = cursor.fetchone()[0]
                print(f"Domain ID created for domain: {row['domain']}")

            # Insert the article into the articles table
            cursor.execute(
                """
                INSERT INTO articles (source_name, author, title, description, url, url_to_image, published_at, content, category, article, title_sentiment, domain_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """,
                (
                    row['source_name'], row['author'], row['title'], row['description'], 
                    row['url'], row['url_to_image'], row['published_at'], row['content'], 
                    row['category'], row['article'], row['title_sentiment'], domain_id
                )
            )

        conn.commit()
        print("Articles data inserted successfully!")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
    finally:
        if conn is not None:
            cursor.close()
            conn.close()

#function to read from the article table
def read_articles():
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cursor = conn.cursor()

        # Execute a query to read data from the 'articles' table
        query = "SELECT * FROM articles;"
        cursor.execute(query)

        # Fetch all rows from the executed query
        rows = cursor.fetchall()

        # Get column names from the cursor description
        column_names = [desc[0] for desc in cursor.description]

        # Convert to a Pandas DataFrame for easy handling
        df = pd.DataFrame(rows, columns=column_names)

        return df

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        return None

    finally:
        # Close the database connection
        if conn is not None:
            cursor.close()
            conn.close()

#function to read from the traffic table
def read_traffic_data():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM traffic_data;")
        traffic_data = cursor.fetchall()

        # Print or return the result as needed
        for traffic in traffic_data:
            print(traffic)

        return traffic_data

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
    finally:
        if conn is not None:
            cursor.close()
            conn.close()


#function to read from the location table
def read_domain_locations():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM domain_locations;")
        locations = cursor.fetchall()

        # Print or return the result as needed
        for location in locations:
            print(location)

        return locations

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
    finally:
        if conn is not None:
            cursor.close()
            conn.close()

#function to read from the location table
def read_domains():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM domains;")
        domains = cursor.fetchall()

        # Print or return the result as needed
        for domain in domains:
            print(domain)

        return domains

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
    finally:
        if conn is not None:
            cursor.close()
            conn.close()




# Run the function to create the database
if __name__ == "__main__":
    create_database()
