"""
Purpose:  Read the death records from CSV files. Records are available from 1866-1992

 Input:     
              
 Effects: Returns death records as a dictionary of lists with keys as the year. 

-- Before running, set fn_stem_record

 Author Paul Moore, 2024 - paul.moore@spi.ox.ac.uk 

 This software is provided 'as is' with no warranty or other guarantee of
 fitness for the user's purpose.  Please let the author know of any bugs
 or potential improvements.
"""

import polars as pl

fn_stem_record = "" 


# compute age given the death record age / date of birth and the year
def find_age(age_in, year):
   
    age_in = age_in.strip()
    age_out = 'NaN'
    
    # later years have the death record age field containing the date of birth viz '8Mr1893'
    if len(age_in) < 4:

        # assume the age is represented directly 
        if age_in.isnumeric():
            age = int(age_in)
            if age >= 0:
                age_out = str(age)
                    
    elif len(age_in) >= 4:
        if age_in[-4:].isnumeric():
            age = year - int(age_in[-4:])
            if age >= 0:
                age_out = str(age)

            

    return(age_out)
      
   # method using month and day 
   # a = dt.datetime.strptime('25052000', "%d%m%Y").date()
   # b = dt.datetime.strptime('26 May 2010', "%d %B %Y").date()
   # return died_date.year - born_date.year - ((died_date.month, died_date.day) < (born_date.month, born_date.day))


# read death records
def read_death_records(start_date, end_date, step=1):
    
    dfdol = {}   # a dictionary of lists of the quarter years, indexed by the year
    str_minor_age = [str(i) for i in range(0,18)]

    for y in range(start_date, end_date+1, step):

        dflist = []
        for i in [1,2,3,4]:
            fn_record = fn_stem_record +  str(y) + "/" + str(y) + "-" + str(i) + ".csv"

            if y < 1984 or i == 1:   # years after 1983 have only a <year>-1.csv file           

                print("Reading " + fn_record + "..")
                
                # some numeric columns have non-numeric entries, and the default encoding of utf-8 is insufficient
                # infer_schema_length forces utf8, missing_utf8_is_empty_string returns the empty string instead of None
                df = pl.read_csv(fn_record, infer_schema_length=0, missing_utf8_is_empty_string=True, encoding='iso8859_15')   

                # make record surname column uppercase, and remove any leading and trailing spaces
                df = df.with_columns(df['surname'].str.to_uppercase())
                df = df.with_columns(df['surname'].str.strip())
                df = df.with_columns(df['givenname'].str.to_uppercase())
                df = df.with_columns(df['givenname'].str.strip())

                # age in some years holds the date of birth
                df = df.with_columns(pl.col("age").apply(lambda a:find_age(a,y)).alias("clean_age"))
                     
            else:
                print("Mapping " + str(y) + "-" + str(i) + "..")  

            # remove minors from dataframe    
            df = df.filter(~pl.col('clean_age').is_in(str_minor_age))
            #df = df.filter(~(pl.col('clean_age') == 'NaN'))

            dflist.append(df)   
        
        dfdol[str(y)] = dflist 

    return(dfdol)




# main
def main():

    # write forenames for use by fix_names
    fn_names_out = "./names/correct_forenames.csv"
    dfdol = read_death_records(1866,1992,2)
#    dfdol = read_death_records(1870,1920,5)

    # build a single dataframe from a dictionary of lists
    dfg = pl.DataFrame()
    for k,dflist in dfdol.items():
        if int(k) <= 1983:
            for df in dflist:
                dfg = pl.concat([dfg, df])
        else:
            dfg = pl.concat([dfg, dflist[0]])

    # extract the  forenames and convert to a list for manipulation
    names = dfg["givenname"].to_list()      

    # take the first portion of the string before any space
    forenames = [x.split()[0] for x in names]
    forenames = [x.lower() for x in forenames]
    forenames = sorted(forenames)

    # filter by the number of occurances
    dfc = pl.DataFrame({"forename": forenames})
    dfc = dfc.groupby("forename").count() 
    #dfc = dfc.filter(pl.col("count") > 10)

    dfc.write_csv(fn_names_out)

# run script
# main()
