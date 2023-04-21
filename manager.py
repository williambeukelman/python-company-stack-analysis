import mariadb
import sys
import json

credentials = {
    "user": "root",
    "password": "password",
    "host": "localhost",
    "port": 3306,
    "database": "scraped_db"
}

def ORC(file):
    ext = file.split('.')[1]
    f = open(file, 'r+')
    o = f.read()
    if ext == 'json': o = json.loads(o)
    f.close()
    return o

def dbCommand(cmd):
    #For simple queries without args
    try:
        cur.execute(cmd)
    except mariadb.Error as e:
        print(f"Error: {e}")

def createOrgTable():
    dbCommand('''
        CREATE TABLE scraped_db.organizations (
            org_id INT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(50),
            website VARCHAR(60),
            hq VARCHAR(50),
            size INT,
            founded INT CHECK (founded < 2100 && founded > 0),
            type VARCHAR(50),
            industry VARCHAR(60),
            revenue VARCHAR(40),
            longdesc VARCHAR(300),
            logo VARCHAR(60),
            stack_verified BOOLEAN,
            stack_shortdesc VARCHAR(800),
            CONSTRAINT FOREIGN KEY (size) REFERENCES scraped_db.org_sizes (size_id);
            ) ENGINE=InnoDB;
        ''')

def createLocationTable():
    dbCommand('''
        CREATE TABLE scraped_db.locations (
            location_id INT PRIMARY KEY AUTO_INCREMENT,
            alias VARCHAR(60),
            city VARCHAR(60),
            state VARCHAR(60),
            country VARCHAR(60)
            ) ENGINE=InnoDB;
        ''')

def createCareerTable():
    dbCommand('''
        CREATE TABLE scraped_db.careers (
            career_id INT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(80)
            ) ENGINE=InnoDB;
        ''')

def createToolTable():
    dbCommand('''
        CREATE TABLE scraped_db.tools (
            tool_id INT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(80),
            category VARCHAR(80),
            subcategory: VARCHAR(60),
            tool_function: VARCHAR(60),
            title: VARCHAR(120),
            description: VARCHAR(255),
            ) ENGINE=InnoDB;
        ''')

def createSocialTable():
    dbCommand('''
        CREATE TABLE scraped_db.socials (
            ref_org_id INT,
            social VARCHAR(120),
            CONSTRAINT `fk_orgsocial_id`
                FOREIGN KEY (ref_org_id) REFERENCES organizations(org_id),
                PRIMARY KEY (ref_org_id, social)
            ) ENGINE=InnoDB;
        ''')

def createAssociateTables():
    for a in [{
        "name": "careers",
        "name2": "organizations",
        "ref1": "career",
        "ref2": "org"
    },{
        "name": "locations",
        "name2": "organizations",
        "ref1": "location",
        "ref2": "org"
    },{
        "name": "tools",
        "name2": "organizations",
        "ref1": "tool",
        "ref2": "org"
    }]:
        dbCommand(f'''
            CREATE TABLE scraped_db.associate_{a['name']} (
                ref_{a['ref1']}_id INT,
                ref_{a['ref2']}_id INT,
                CONSTRAINT `fk_{a['ref1']}_id`
                FOREIGN KEY (ref_{a['ref1']}_id) REFERENCES {a['name']}({a['ref1']}_id),
                CONSTRAINT `fk_{a['ref2']+a['ref1']}_id`
                FOREIGN KEY (ref_{a['ref2']}_id) REFERENCES {a['name2']}({a['ref2']}_id),
                PRIMARY KEY (ref_{a['ref1']}_id, ref_{a['ref2']}_id)
                ) ENGINE=InnoDB;
            ''')

def insertOrgs():
    dbCommand("SELECT size_id , numerical FROM scraped_db.org_sizes")
    sizeMap = {}
    for _id, numerical in cur:
        sizeMap[numerical] = _id
    companies = ORC("db_companies.json")
    keys = ['name', 'website', 'hq', 'size', 'founded', 'type', 'industry', 'revenue', 'longdesc', 'logo', 'stack_verified', 'stack_shortdesc']
    for c in companies:
        data = {}
        for key in keys:
            if(key == 'stack_verified' and c[key] == ""):
                data[key] = 0
            elif(key == 'size' and c[key] != 'null'):
                data[key] = sizeMap[c[key]]
            elif(key == 'size' and c[key] == 'null'):
                data[key] = sizeMap['Unknown']
            elif(c[key] == "null" or c[key] == "None" or c[key] == ""):
                data[key] = None
            else:
                data[key] = c[key]
        try: 
            cur.execute('''INSERT INTO organizations (name, website, hq, size, founded, type, industry, revenue, longdesc, logo, stack_verified, stack_shortdesc)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
            , ([data[i] for i in keys])) 
        except mariadb.Error as e: 
            print(f"Error: {e}")
            print(c)

def insertGeneric(file_name, table_name, keyList):
    data = ORC(file_name)
    keys = keyList
    for element in data:
        records = {}
        for key in keys:
            if(element[key] == "null" or element[key] == "None" or element[key] == ""):
                records[key] = None
            else:
                records[key] = element[key]
        try: 
            cur.execute(f'INSERT INTO {table_name} ({", ".join(keyList)}) ' + 'VALUES ({})'.format(", ".join(["?" for x in keyList]))
            , ([records[i] for i in keys])) 
        except mariadb.Error as e: 
            print(f"Error: {e}")
            print(element)  

def createMappings(ids, names, tables):
    maps = {}
    for table in tables:
        cur.execute(f'SELECT {ids[table]}, {names[table]} FROM scraped_db.{table}')
        maps[table] = {}
        for (_id, name) in cur:
            maps[table][name] = _id
    return maps

def insertAssociations():
    #Create Mappings
    tables = ['tools', 'organizations', 'careers', 'locations']
    ids = {'tools': 'tool_id', 'organizations': 'org_id', 'careers': 'career_id', 'locations': 'location_id'}
    names = {'tools': 'name', 'organizations': 'name', 'careers': 'name', 'locations': 'alias'}
    maps = createMappings(ids, names, tables)
    #Process Files
    for file_name in ['careers', 'locations', 'tools']:
        data = ORC(f'db_associate_{file_name}.json')
        _name = file_name[:-1]
        for pair in data:
            try:
                id1 = maps['organizations'][pair['name']]
            except:
                print(pair['name'], " not in db orgs")
            id2 = maps[file_name][pair[_name]]
            try: 
                cur.execute(f'''INSERT IGNORE INTO associate_{file_name} (ref_org_id, ref_{_name}_id)
                VALUES (?, ?)'''
                , (id1, id2)) 
            except mariadb.Error as e: 
                print(f"Error: {e}")
                print(pair)
    #Socials
    data = ORC(f'db_associate_socials.json')
    for pair in data:
            try:
                id1 = maps['organizations'][pair['name']]
            except:
                print(pair['name'], " not in db orgs")
            social = pair['social']
            try: 
                cur.execute(f'''INSERT IGNORE INTO socials (ref_org_id, social)
                VALUES (?, ?)'''
                , (id1, social)) 
            except mariadb.Error as e: 
                print(f"Error: {e}")
                print(pair)
    return

#Patch Commands
def patchTools():
    #Deprecated as create script has been updated.
    dbCommand('''
        ALTER TABLE scraped_db.tools
        ADD COLUMN subcategory VARCHAR(60),
        ADD COLUMN tool_function VARCHAR(60),
        ADD COLUMN title VARCHAR(300),
        ADD COLUMN description VARCHAR(700);
        ''')
    print(f'Tools has been patched.')
    data = ORC('db_tools_improved.json')
    for item in data:
        try:
            if(item['title']): title = item['title'].replace('"', "'")
            else: title = None
            if(item['description']): description = item['description'].replace('"', "'")
            else: description = None
            cur.execute(f'''
            UPDATE scraped_db.tools SET 
            subcategory = "{item['subcategory']}", 
            tool_function = "{item['function']}",
            title = "{title}",
            description = "{description}"
            WHERE name = "{item['name']}";
            ''')
        except mariadb.Error as e:
            print(f'Error: {e}')
    return

def patchCompanySize():
    #Deprecated as create script has been updated.
    #Create company_size table

    dbCommand('''
    CREATE TABLE IF NOT EXISTS scraped_db.org_sizes (
        size_id INT PRIMARY KEY AUTO_INCREMENT,
        numerical VARCHAR(20),
        categorical VARCHAR(30)
        ) ENGINE=InnoDB;
    ''')
    dbCommand('''
    INSERT INTO org_sizes (numerical, categorical)
    VALUES ('Unknown', 'Unknown'),
    ('1 to 50', 'Micro'),
    ('51 to 200', 'Small'),
    ('201 to 500', 'Midsize'),
    ('501 to 1000', 'Large'),
    ('1001 to 5000', 'Very Large'),
    ('5001 to 10000', 'Enterprise'),
    ('10000+', 'Large Enterprise');
    ''')
    #Get all the referece ids and put them in dictionary
    dbCommand('''
        SELECT org_id, size_id  
        FROM scraped_db.organizations o 
        JOIN scraped_db.org_sizes os ON os.numerical = o.size WHERE size IS NOT NULL;
        ''')
    data = {}
    for _id, size_id in cur:
        data[_id] = size_id
    #Recreate column with appropriate datatype
    dbCommand('''
        ALTER TABLE scraped_db.organizations
        DROP COLUMN size;
        ''')
    dbCommand('''
        ALTER TABLE scraped_db.organizations
        ADD COLUMN size INT;
        ''')
    #Iterate over and update values in organization table
    for _id, size_id in data.items():
        dbCommand(f'''
            UPDATE scraped_db.organizations SET 
            size = {size_id}
            WHERE org_id = {_id};
            ''')
    dbCommand('''
    ALTER TABLE scraped_db.organizations
    ADD CONSTRAINT FOREIGN KEY (size) REFERENCES scraped_db.org_sizes (size_id);
    ''')
    return

def initializeDatabase():
    dbCommand("CREATE DATABASE IF NOT EXISTS scraped_db; ")    
    #Check what needs created if anything
    try:
        cur.execute('''
        SELECT TABLE_NAME
        FROM information_schema.TABLES 
        WHERE 
            TABLE_SCHEMA LIKE 'scraped_db' AND 
            TABLE_TYPE LIKE 'BASE TABLE'
        ''')
        tables = []
        for table in cur:
            tables.append(*table)
        if('org_sizes' not in tables):
            dbCommand('''
            CREATE TABLE scraped_db.org_sizes IF NOT EXISTS (
                size_id INT PRIMARY KEY AUTO_INCREMENT,
                numerical VARCHAR(20),
                categorical VARCHAR(30)
                ) ENGINE=InnoDB;
            ''')
            dbCommand('''
            INSERT INTO org_sizes (numerical, categorical)
            VALUES ('Unknown', 'Unknown'),
            ('1 to 50', 'Micro'),
            ('51 to 200', 'Small'),
            ('201 to 500', 'Midsize'),
            ('501 to 1000', 'Large'),
            ('1001 to 5000', 'Very Large'),
            ('5001 to 10000', 'Enterprise'),
            ('10000+', 'Large Enterprise');
            ''')
            print("Created 'org_sizes' table")
        if('organizations' not in tables):
            createOrgTable()
            print("Created 'organizations' table")
            insertOrgs()
        if('locations' not in tables):
            createLocationTable()
            print("Created 'locations' table")
            insertGeneric('db_locations.json', 'locations', ['alias', 'city', 'state', 'country'])
        if('careers' not in tables):
            createCareerTable()
            print("Created 'careers' table")
            insertGeneric('db_careers.json', 'careers', ['name'])
        if('tools' not in tables):
            createToolTable()
            print("Created 'tools' table")
            insertGeneric('db_tools.json', 'tools', ['name', 'category', 'subcategory', 'tool_function', 'title', 'description'])
        if('socials' not in tables
            or 'associate_careers' not in tables
            or 'associate_tools' not in tables
            or 'associate_locations' not in tables
        ):
            createSocialTable()
            createAssociateTables()
            print("Created association tables")
            insertAssociations()
        for table in tables:
            print(f"\033[92m \u2713 Table '{table}' already exists \033[0m")
    except mariadb.Error as e: 
        print(f"Error: {e}")

if(__name__ == "__main__"):
    # Connect to MariaDB Platform
    try:
        conn = mariadb.connect(
            user = credentials['user'],
            password = credentials['password'],
            host = credentials['host'],
            port = credentials['port'],
            database = credentials['database']
        )
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)

    # Get Cursor
    cur = conn.cursor()

    #initializeDatabase()
    #patchTools()
    #patchCompanySize()

    conn.commit() 
    print(f"Last Inserted ID: {cur.lastrowid}")
        
    conn.close()

