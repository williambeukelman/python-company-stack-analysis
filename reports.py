import numpy as np
import pandas as pd
import matplotlib as mpl
from matplotlib.font_manager import fontManager, FontProperties
import matplotlib.pyplot as plt
import seaborn as sns
import seaborn.objects as so
import mariadb
import sys

path = "LibreBaskerville-Regular.ttf"#"Lato-Hairline.ttf"#
fontManager.addfont(path)

prop = FontProperties(fname=path)
#sns.set(font=prop.get_name())

# Connect to MariaDB Platform
try:
    conn = mariadb.connect(
        user="root",
        password="password",
        host="localhost",
        port=3306,
        database="scraped_db"
    )
except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)

# Get Cursor
cur = conn.cursor()

def dbCommand(cmd):
    #For simple queries without args
    try:
        cur.execute(cmd)
    except mariadb.Error as e:
        print(f"Error: {e}")

dbCommand("USE `scraped_db`; ")

def Figure1():
    #Histogram stacked with colors
    #Organizations by Founded Date and Grouped by Size
    dbCommand("SELECT org_id, founded, categorical FROM organizations o JOIN org_sizes os ON o.size = os.size_id WHERE founded IS NOT NULL;")
    data = {'_id': [], 'founded': [], 'categorical': [],}
    for _id, founded, categorical in cur:
        data['_id'].append(_id)
        data['founded'].append(founded)
        data['categorical'].append(categorical)
    df = pd.DataFrame.from_dict(data)

    f, ax = plt.subplots(figsize=(7, 5))
    sns.despine(f)

    g = sns.histplot(
        df,
        x="founded", hue="categorical",
        palette="rocket",
        multiple="stack",
        edgecolor=".3",
        linewidth=.5,
        log_scale=False,
    )
    g.set_title('Organizations by Founded Date and Grouped by Size', fontdict={'family': prop.get_name(), 'size': 14})
    
    ax.xaxis.set_units("Year")
    g.figure.savefig("./Figures/Figure1.svg") 
    #plt.show()

def Figure2():
    dbCommand('''
        SELECT t.name, categorical, t.category
        FROM organizations o 
        JOIN org_sizes os ON o.size = os.size_id 
        JOIN associate_tools at2 ON o.org_id = at2.ref_org_id 
        JOIN tools t ON at2.ref_tool_id = t.tool_id 
        WHERE categorical != 'Unknown'
        ORDER BY categorical DESC
    ''')
    data = {'name': [], 'categorical': [], 'category': []}
    for name, categorical, category in cur:
        data['name'].append(name)
        data['categorical'].append(categorical)
        data['category'].append(category)
    df = pd.DataFrame.from_dict(data)
    def countplot(*args, **kwargs):
        colName = kwargs['data']['categorical'].iloc[0]
        sns.countplot(y='name', hue='category', data=kwargs['data'], order=pd.value_counts(kwargs['data']['name']).iloc[:10].index, palette="colorblind", dodge=False, orient='h')#.set_yticklabels(labels=)

    g = sns.FacetGrid(df, col="categorical", sharey=False, col_wrap=3, legend_out=False, col_order=['Micro', 'Small', 'Midsize', 'Large', 'Very Large', 'Enterprise', 'Large Enterprise'])
    g.map_dataframe(countplot)
    g.set_titles(col_template="{col_name}", row_template="{row_name}")
    g.figure.subplots_adjust(wspace=1.25, hspace=.5, top=0.912)
    g.figure.suptitle('Top 10 Techologies by Company Size', fontdict={'family': prop.get_name(), 'weight': 'bold', 'size': 30})

    g.set_ylabels("")
    for axis in g.axes.flat:
        axis.tick_params(labelleft=True, labelbottom=True)
    plt.legend(bbox_to_anchor=(2.02, 1.15), loc='upper left', borderaxespad=0)
    g.figure.savefig("./Figures/Figure2.svg") 

def Figure3():
    #Top 20 Tools
    dbCommand('''
        SELECT t.name, categorical, t.category, t.subcategory, t.tool_function 
        FROM organizations o 
        JOIN org_sizes os ON o.size = os.size_id 
        JOIN associate_tools at2 ON o.org_id = at2.ref_org_id 
        JOIN tools t ON at2.ref_tool_id = t.tool_id 
        WHERE categorical != 'Unknown' AND t.category = 'Application and Data'
        ORDER BY categorical DESC
    ''')
    data = {'name': [], 'categorical': [], 'category': [], 'subcategory': [], 'tool_function': []}
    for name, categorical, category, subcategory, tool_function in cur:
        data['name'].append(name)
        data['categorical'].append(categorical)
        data['category'].append(category)
        data['subcategory'].append(subcategory)
        data['tool_function'].append(tool_function)
    df = pd.DataFrame.from_dict(data)
    f, ax = plt.subplots(figsize=(7, 5))
    sns.despine(f)

    g = sns.countplot(y='name', hue='subcategory', data=data, order=pd.value_counts(data['name']).iloc[:20].index, palette="colorblind", dodge=False)#.set_yticklabels(labels=)
    g.set_title('Top 20 Tools', fontdict={'family': prop.get_name(), 'size': 14})
    g.figure.subplots_adjust(wspace=1.25, hspace=.5, left=.2, top=0.912)
    plt.show()

def dbCallToDict(q, keys):
    dbCommand(q)
    data = {}
    for key in keys:
        data[key] = []
    for args in cur:
        for i, key in enumerate(keys):
            data[key].append(args[i])
    df = pd.DataFrame.from_dict(data)
    return df

def Figure4():
    #Top 20 Tools
    q = '''
        SELECT t.name, categorical, t.category, t.subcategory, t.tool_function 
        FROM organizations o 
        JOIN org_sizes os ON o.size = os.size_id 
        JOIN associate_tools at2 ON o.org_id = at2.ref_org_id 
        JOIN tools t ON at2.ref_tool_id = t.tool_id 
        WHERE categorical != 'Unknown' AND t.tool_function = 'Languages'
        ORDER BY categorical DESC
    '''
    df = dbCallToDict(q, ['name', 'categorical', 'category', 'subcategory', 'tool_function'])
    f, ax = plt.subplots(figsize=(7, 5))
    sns.despine(f)

    g = sns.countplot(y='name', data=df, order=pd.value_counts(df['name']).iloc[:20].index, palette="Blues_r", dodge=False)#.set_yticklabels(labels=)
    g.set_title('Top 20 Languages', fontdict={'family': prop.get_name(), 'size': 14})
    g.figure.subplots_adjust(wspace=1.25, hspace=.5, left=.2, top=0.912)
    #plt.show()
    g.figure.savefig("./Figures/Figure4.svg")

def Figure5():
    #Top Database
    q = '''
        SELECT t.name, categorical, t.category, t.subcategory, t.tool_function 
        FROM organizations o 
        JOIN org_sizes os ON o.size = os.size_id 
        JOIN associate_tools at2 ON o.org_id = at2.ref_org_id 
        JOIN tools t ON at2.ref_tool_id = t.tool_id 
        WHERE categorical != 'Unknown' AND t.tool_function = 'Databases'
        ORDER BY categorical DESC
    '''
    df = dbCallToDict(q, ['name', 'categorical', 'category', 'subcategory', 'tool_function'])
    f, ax = plt.subplots(figsize=(7, 5))
    sns.despine(f)

    g = sns.countplot(y='name', data=df, order=pd.value_counts(df['name']).iloc[:20].index, palette="Reds_r", dodge=False)#.set_yticklabels(labels=)
    g.set_title('Top Databases', fontdict={'family': prop.get_name(), 'size': 14})
    g.figure.subplots_adjust(wspace=1.25, hspace=.5, left=0.28, top=0.912)
    #plt.show()
    g.figure.savefig("./Figures/Figure5.svg")

def Figure6():
    dbCommand('''
    SELECT DISTINCT t.tool_function 
    FROM tools t
    ''')
    toolset = []
    for tool in cur:
        toolset.append(tool[0])
    for index, tool_function in enumerate(toolset):
        #Top of each Tool Function
        print(index, tool_function)
        try:
            q = f'''
                SELECT t.name, categorical, t.category, t.subcategory, t.tool_function 
                FROM organizations o 
                JOIN org_sizes os ON o.size = os.size_id 
                JOIN associate_tools at2 ON o.org_id = at2.ref_org_id 
                JOIN tools t ON at2.ref_tool_id = t.tool_id 
                WHERE categorical != 'Unknown' AND t.tool_function = '{tool_function}'
                ORDER BY categorical DESC
            '''
            df = dbCallToDict(q, ['name', 'categorical', 'category', 'subcategory', 'tool_function'])
            if((df.size-5)>2):
                f, ax = plt.subplots(figsize=(7, 5))
                sns.despine(f)
                g = sns.countplot(y='name', data=df, order=pd.value_counts(df['name']).iloc[:20].index, palette="Blues_r", dodge=False)#.set_yticklabels(labels=)
                g.set_title(f'Top {tool_function}', fontdict={'family': prop.get_name(), 'size': 14})
                g.figure.subplots_adjust(wspace=1.25, hspace=.5, left=0.28, top=0.912)
                g.figure.savefig(f"./Figures/All_Tool_Functions/Figure{index}.svg")
                g.figure.savefig(f"./Figures/All_Tool_Functions/Previews/Figure{index}.png")
                plt.close()
            else: break
        except Exception as e:
            print("Error: ", e)

def Figure7():
    dbCommand('''
    SELECT DISTINCT t.subcategory 
    FROM tools t
    ''')
    toolset = []
    for tool in cur:
        toolset.append(tool[0])
    for index, subcategory in enumerate(toolset):
        #Top of each Tool Function
        print(index, subcategory)
        try:
            q = f'''
                SELECT t.name, categorical, t.category, t.subcategory, t.tool_function 
                FROM organizations o 
                JOIN org_sizes os ON o.size = os.size_id 
                JOIN associate_tools at2 ON o.org_id = at2.ref_org_id 
                JOIN tools t ON at2.ref_tool_id = t.tool_id 
                WHERE categorical != 'Unknown' AND t.subcategory = '{subcategory}'
                ORDER BY categorical DESC
            '''
            df = dbCallToDict(q, ['name', 'categorical', 'category', 'subcategory', 'tool_function'])
            if((df.size-5)>2):
                f, ax = plt.subplots(figsize=(7, 5))
                sns.despine(f)
                g = sns.countplot(y='name', hue='tool_function', data=df, order=pd.value_counts(df['name']).iloc[:20].index, palette="colorblind", dodge=False)#.set_yticklabels(labels=)
                g.set_title(f'Top {subcategory}', fontdict={'family': prop.get_name(), 'size': 14})
                g.figure.subplots_adjust(wspace=1.25, hspace=.5, left=0.28, top=0.912)
                g.figure.savefig(f"./Figures/All_Subcategories/Figure{index}.svg")
                g.figure.savefig(f"./Figures/All_Subcategories/Previews/Figure{index}.png")
                plt.close()
            else: break
        except Exception as e:
            print("Error: ", e)

if(__name__ == "__main__"):
    Figure7()