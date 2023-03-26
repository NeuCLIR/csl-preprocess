from tqdm.auto import tqdm
import json
import pandas as pd

if __name__ == '__main__':
    data = [ l.strip().split('\t') for l in open('csl_dataset.tsv') ]
    data = pd.DataFrame(data, columns=['title', 'abstract', 'keywords', 'discipline', 'category'])
    
    # data = raw_data.rename(columns={'discipline': 'discipline_zho', 'category': 'category_zho'})
    trans_mapping = json.load(open('category_mapping.json'))
    
    data = data.assign(discipline_eng=data.discipline.map(trans_mapping), category_eng=data.category.map(trans_mapping))
    data = data.applymap(str.strip)
    data = data.assign(keywords=data.keywords.str.split("_"), doc_id=data.index.map(lambda x: f'csl-{x:06d}')).set_index('doc_id')

    exact_dups = data.reset_index().groupby('abstract').doc_id.apply(list).reset_index(drop=True)
    
    # Remove entries duplicated abstracts -- 
    # There are some entries with the same abstract but different titles.
    # They are generally: 
    # (1) sequels broken down into multiple entries with clear ordering labels in the title, e.g. I, II 
    # (2) potential resubmissions with very minor difference in the title and identical metadata.
    print(f"Found {exact_dups.shape[0]} abstract groups, with { (exact_dups.str.len() > 1).sum() } groups with more than one entry.")
    data_dups_removed = data.loc[ exact_dups.apply(lambda x: x[0]).tolist() ]

    with open('csl_data_dedups.jsonl', 'w') as fw:
        for _ , row in tqdm(data_dups_removed.reset_index()[['doc_id', 'title', 'abstract', 'keywords', 'category', 'category_eng', 'discipline', 'discipline_eng']].iterrows()):
            fw.write(json.dumps(row.to_dict()) + '\n')
