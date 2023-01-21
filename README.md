# demo-task-res-tec
## Simple solution to find and sort by name people with equal addresses.
### For raw address matching is used function `token_set_ratio()` from package "RapidFuzz". The string is tokenized and then, 
### sorting and then pasting the tokens back together, token_set_ratio performs a set operation that 
### takes out the common tokens (the intersection) and then makes fuzz.ratio() pairwise comparisons 
### between the following new strings:
### s1 = Sorted_tokens_in_intersection
### s2 = Sorted_tokens_in_intersection + sorted_rest_of_str1_tokens
### s3 = Sorted_tokens_in_intersection + sorted_rest_of_str2_tokens
### Then the Google Places API with unique addresses is used for final address matching. 
### The initial matchig with `token_set_ratio()` helps to reduce calls to Google's API, if matches are found.
## Instaling dependencies 
### There are two possiblle approaches 
#### 1. Creating venv with `python -m venv venv`, activate and then `pip install -r requirements.txt`
#### 2. Using pipenv - `pip install pipenv`, `pipenv install --dev` and activate `pipenv shell` from project directory.
## Running script
### From root project directory run `python run.py`. On first run files 'result.csv' and 'log.log' should be created.


