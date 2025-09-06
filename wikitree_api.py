"""
 Purpose:  WikiTree API caller
     
           The WikiTree API documentation is at https://github.com/wikitree/wikitree-api          
               
 Input:     
              
 Effects: 

 Author Paul Moore, 2024 - paul.moore@spi.ox.ac.uk 

 This software is provided 'as is' with no warranty or other guarantee of
 fitness for the user's purpose.  Please let the author know of any bugs
 or potential improvements.
"""

import time
import regex as re
import requests
import polars as pl

# build URL
def build_url(parms):

    urlbase = "https://api.wikitree.com/api.php?"

    for k,v in parms.items():
        urlbase = urlbase + k + '=' + v + '&'
    urlbase = urlbase[:-1]    

    return(urlbase)



# search for a person using a WikiTree ID or a User ID
def get_person(id):

    # example URL
    # https://api.wikitree.com/api.php?action=getPerson&key=Young-26811&fields=Id,Name,DeathDate

    parms = {
        'action': 'getPerson',
        'key': id,
        'fields': 'Id,Name,FirstName,LastNameCurrent,BirthDate,DeathDate,DeathLocation,Father,Mother'
    }
    
    url = build_url(parms)

    time.sleep(0.05)

    r = requests.get(url)

    profile = r.json()[0]['person']

    return(profile)



# search for a WikiTree profile using a WikiTree ID (does not take a User ID)
def get_profile(wt_id):

    # example URL
    # https://api.wikitree.com/api.php?action=getProfile&key=Young-26811&fields=Id,Name,DeathDate

    parms = {
        'action': 'getProfile',
        'key': wt_id,
        'fields': 'Id,Name,FirstName,LastNameCurrent,DeathDate,DeathLocation'
        #'fields': '*'
    }
    
    url = build_url(parms)

    r = requests.get(url)

    profile = r.json()[0]['profile']

    return(profile)


# search for parents using WikiTree PageID
def get_parents(wt_id):

    # example URL
    # https://api.wikitree.com/api.php?action=getAncestors&key=Young-35459&depth=1&fields=Id,Name,Mother,Father


    parms = {
        'action': 'getAncestors',
        'key': wt_id,
        'depth': '1',
        'resolveRedirect': '1',
        'fields': 'Id,Name,Mother,Father'
    }
    
    url = build_url(parms)

    r = requests.get(url)

    b = r.json()[0]['ancestors']

    # assert two parents
    assert(len(b) == 3)             
    
    father = b[1]['Name']
    mother = b[2]['Name']
    

    profile_father = get_profile(father)
    profile_mother = get_profile(mother)

    out = [profile_father, profile_mother]

    return(out)


# simple case sensitive search
def isin(needle, haystack):
    a = re.search(needle, haystack)
    return(a is not None)



# search for individual using name and DoD returning a list of dicts
def search_person_custom(firstname, lastname, birthlocation, deathyear):
   
    # docs at https://github.com/wikitree/wikitree-api/blob/main/searchPerson.md
    # see also the WikiTree web interface -  https://www.wikitree.com/ - search then edit search
    # return fields are documented in https://github.com/wikitree/wikitree-api/blob/main/getProfile.md

    deathdate = f"{deathyear}-07-02"  # choose the middle of the year - see dateSpread below
    parms = {
        'action': 'searchPerson',
        'FirstName': firstname,
        'LastName': lastname,
        'DeathDate': deathdate,
        'BirthLocation': birthlocation,
        'dateInclude': 'both',      # require dates on matched profiles ('both' can still return "0000-00-00")
        'dateSpread': '1',          # spread of years for date matches ('1' returns deathyear +- 1)
        'centuryTypo': '0',         # '1' includes possible century typos in date matches
        'skipVariants': '1',        # skip variant last names in matches
        'lastNameMatch': 'birth',   # 'all' includes birth and married/current last name matches
        'sort': 'last',             # sort order
        'secondarySort': 'first',   # secondary sort order
        'limit': '100',             # number of results to return 
        'start': '<START>',         # starting offset of return set  
        'fields': 'Id,Name,FirstName,LastNameCurrent,BirthDate,DeathDate,BirthLocation,DeathLocation,Father,Mother'  # fields to return
    }
    
    url_search = build_url(parms)

    offset = 0
    block_size = 100
    go = True
    rs = []

    # fetch blocks of size block_size
    while go:

        url = url_search
        url = url.replace('<START>',str(offset))
        
        time.sleep(0.05)
        
        r = requests.get(url)
        total = r.json()[0]['total']
        rs.append(r)
        offset += block_size
        go = offset < total

    people = []

    # display result set
    #js = [j for j in i['matches'] for i in r.json() for r in rs]

    for r in rs:
        for i in r.json():
            for j in i['matches']:

                if ('DeathDate' in j):
                    if  (j['DeathDate'][0:4] == deathyear):
                            
                        # the birth location is a parameter, added primarily as a speedup    
                        lookfor = "England|Scotland|Wales|Northern Ireland"
#                        chk1 = (isin(lookfor,j['BirthLocation'])) and (isin(lookfor,j['DeathLocation']))  
                        chk1 = isin(lookfor,j['DeathLocation'])

                        # ignore is needed owing to eg. New South Wales and New England etc.
                        ignore = "United States|Australia|USA|Canada"
                        chk2 = (not isin(ignore,j['BirthLocation'])) and (not isin(ignore,j['DeathLocation']))  

                        chk3 = (j['Father'] != 0 or j['Mother'] != 0)

                        if chk1 and chk2 and chk3: 
                            people.append({
                                    'wt_id': j['Name'], 
                                    'FirstName': j['FirstName'], 
                                    'LastNameCurrent': j['LastNameCurrent'],
                                    'BirthDate': j['BirthDate'], 
                                    'DeathDate': j['DeathDate'], 
                                    'BirthLocation': j['BirthLocation'], 
                                    'DeathLocation': j['DeathLocation'], 
                                    'Father': str(j['Father']), 
                                    'Mother': str(j['Mother']),
                                    })     

    return((people, total))





# main
def main():

    # test code
    if 1:
        firstname = "*"
        lastname = "moore"
        deathdate = "1960"
        birthlocation = "England"
        (people,total) = search_person_debug(firstname, lastname, birthlocation, deathdate)    
        print(f'Total: {total}')
        df = pl.DataFrame(people)
        df.write_csv('./results/wikitree_debug.csv')


        

#run script
start_time = time.time()
main()
print(f"Time taken {(time.time() - start_time):.2f} seconds")





