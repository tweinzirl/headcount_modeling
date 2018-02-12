import pandas as pd

df = pd.read_csv('localOutput/adf.csv')

#mapper of employee id to manager
#map_dict = df[['Client_ID','Client_ID_MANAGER']].to_dict('list')
#mapper = {}
#for a,b in zip()

##why use dict when can just look up through the dataframe
map_df = df[['Client_ID','Client_ID_MANAGER']].set_index('Client_ID')
emp = map_df.index.values.copy()
mappers = []

for i in range(1,8):
    #http://pandas.pydata.org/pandas-docs/stable/indexing.html#deprecate-loc-reindex-listlike
    man = map_df.loc[map_df.index.intersection(emp),'Client_ID_MANAGER'].reindex(emp).dropna().unique()
    print('level %d - num employees: %d  num managers: %d'%(i,len(emp),len(man)))

    emp = man.copy()

    #retrieve emp with these man
    rel_df = map_df[map_df.Client_ID_MANAGER.isin(man)]
    map_dict = rel_df.to_dict()
    #mapper={}
    #for a,b in zip(map_dict['Client_ID'], map_dict['Client_ID_MANAGER']): mapper[a]=b
    mappers.append(map_dict['Client_ID_MANAGER'])

management_chain = {}
for emp, man in mappers[0].items():
    chain = [man] #chain is constructed from the bottom up
    for rd in mappers[1:]:
        if man in rd and rd[man]!=man:
            next_man = rd[man]
            chain.append(next_man)
            man = next_man
        else:
            break
            #chain.append(np.nan)
    management_chain[emp] = chain

management_chain_topdown = management_chain.copy()
for key, chain in management_chain.items()[:]:
    chain2 = chain[::-1] + [np.nan]*(7-len(chain)) #reverse the chain to get top down view and pad undefined levels with nan
    print(chain, chain2)
    management_chain_topdown[key] = chain2

df = pd.DataFrame.from_dict(management_chain_topdown, orient='index')
df = df.rename({0:'PA_Leadership_Level_1',1:'PA_Leadership_Level_2',2:'PA_Leadership_Level_3',\
    3:'PA_Leadership_Level_4',4:'PA_Leadership_Level_5',5:'PA_Leadership_Level_6',6:'PA_Leadership_Level_7'},axis='columns')
