import os
import pandas as pd
import sqlalchemy
import random
import numpy as np

from sqlalchemy.orm import sessionmaker
from faker import Faker
from sqlite_main import SQLiteRepo

class SQLitePipeline(SQLiteRepo):
    def __init__(self, connect: str):
        # call parent to set up engine and connect inheritace
        super().__init__(connect)

    @staticmethod
    def gen_phone_number(num_obs):
        """
        Generate 10 digits telephone numbers 
    
        Args:
            num_obs (int): Number of total telephone numbers to generate
    
        Returns:
            List of inidividual items
        """
    
        county_code = "+233"
        prefix = ["020", "054", "026", "024", "050", "055"]
        data = []
        for _ in range(num_obs):
            phone_num = (
                random.choice(prefix) +
                "".join(str(random.randint(0,9)) for _ in range(7)) 
            )
            
            data.append(phone_num)
        return data
        
    def generate_crops_data(self):
        """
        generate crops records from mapped dictionary.
    
        Args:
            none
    
        Return:
            pd.DataFrame: Generated crop_plants records
        """
        value_map = {
            "maize": "grains",
            "beans": "legumes",
            "yam": "tubber",
            "tomato": "vegetables",
            "rice": "grains",
            "cassava": "tubber",
            "onion": "vegetables",
            "plantain": "fruits",
            "pepper": "vegetables",
            "groundnut": "legumes"
        }
        data = []
        for key, value in value_map.items():
            data.append({
                "crop_name": key,
                "category": value
            })
        df = pd.DataFrame(data)
        return df
    

    def generate_crop_plants_data(self, n_obs):
        """
        Generate crop_plants records with uneven crop_id distribution.
        
        Args:
            n_obs (int): Number of planting records to generate.
        
        Returns:
            pd.DataFrame: Generated crop_plants records.
        """
        random.seed(42)
        col_name = ["crop_id", "region", "plant_date"]
        region = ["Nothern-belt", "Middle-belt", "Southern-belt"]
        date = pd.bdate_range("2024-4-1", "2024-8-31",freq="1D")
        date_weights = [5 if _ < 0.7 * len(date) else 2.5 for _ in range(len(date))]
        
        # query ids from parent table
        with self.engine.connect() as conn:
            valid_ids = pd.read_sql("SELECT crop_id FROM crops", conn)

        # extract ids and assign weights
        valid_ids = valid_ids["crop_id"].to_list()
        id_weights = np.random.dirichlet(np.ones(len(valid_ids)), size=20)[0]
        choosen_id = np.random.choice(valid_ids, size=n_obs, p=id_weights)
        # impute data 
        data = []
        for _ in range(n_obs):
            data.append(
                {
                    "crop_id": random.choice(choosen_id),
                    "region": random.choice(random.choices(region, weights=[0.37, 0.35, 0.27], k=20)),
                    "plant_date": random.choices(date, weights=date_weights, k=20)[0].date(),
                }
            )
        df = pd.DataFrame(data)
        return df
        

    def gen_damage_report_data(self, n_obs):
        """
        Generate crop_plants records with uneven  distribution.
        
        Args:
            n_obs (int): Number of damage records to generate.
        
        Returns:
            pd.DataFrame: Generated damage_report records.
        """
        random.seed(42)
        damage = ["disease", "drought", "pest", "flood", "others"]
        severity = random.choices(list(range(1, 11)), weights=[5, 4, 3, 3, 2, 2, 1, 1, 1, 1], k=20)
        date = pd.bdate_range("2024-10-1", "2025-4-30", freq="3D")
        date_weights = [5 if _ < 0.35 * len(date) else 3.5 for _ in range(len(date))] 
        # query data from parent table by count
        def __gen_ids(n_records):
            random.seed(42)
            # Step 1: Query planting frequency grouped by crop_id
            with self.engine.connect() as conn:
                freq_df = pd.read_sql(
                    ("""
                        SELECT c.crop_id, c.crop_name, COUNT(p.plant_id) as freq
                        FROM crops c
                        JOIN crop_plants p ON c.crop_id = p.crop_id
                        GROUP BY c.crop_id, c.crop_name
                    """),
                    conn
                )
        
            crop_ids = freq_df["crop_id"].tolist()
            freqs = freq_df["freq"].tolist()
        
            # Step 2: Normalize frequencies into crop-level weights
            crop_weights = np.array(freqs) / np.sum(freqs)
        
            # Step 3: Sample crop_ids based on planting frequency
            chosen_crop_ids = np.random.choice(crop_ids, size=n_records, p=crop_weights)
        
            # Step 4: For each crop_id, pick a valid plant_id
            with self.engine.connect() as conn:
                plants_df = pd.read_sql("SELECT plant_id, crop_id FROM crop_plants", conn)
        
            chosen_plant_ids = []
            for crop_id in chosen_crop_ids:
                valid_plants = plants_df[plants_df["crop_id"] == crop_id]["plant_id"].tolist()
                chosen_plant_ids.append(np.random.choice(valid_plants))
    
            return chosen_plant_ids
            
        plant_ids = __gen_ids(n_obs)
        # append data
        data = []
        for _ in range(n_obs):
            data.append({
                "plant_id": random.choice(plant_ids),
                "damage_type": random.choice(random.choices(damage, weights=[0.35, 0.3, 0.2, 1, 0.05], k= 20)),
                "severity": random.choice(severity),
                "report_date": random.choices(date, weights=date_weights, k=1)[0].date()
                
            })
        
        df = pd.DataFrame(data)
        return(df)

    
    def gen_inspections_data(self, n_obs):
        """
        Generate inspections records with uneven  distribution.
        
        Args:
            n_obs (int): Number of damage records to generate.
            engine : SQLAlchemy engine object.
        
        Returns:
            pd.DataFrame: Generated inspections records.
        """
        random.seed(42)
        fk = Faker()
        col_name = ['inspector_id', 'report_id', 'notes', 'inspection_date']
        date = pd.bdate_range("2024-10-15", "2025-8-31")
        weights = [len(date) - _ for _ in range(len(date))]
        
        word_list = ["pest infestation", "fungal infection", "bacterial blight", "viral disease", "root rot", "leaf spot",
                    "stem borer",
                    "weevil attack",
                    "aphid damage",
                    "mite infestation",
                    "drought stress",
                    "frost injury",
                    "hail damage",
                    "nutrient deficiency",
                    "soil erosion",
                    "waterlogging",
                    "sun scorch",
                    "wind damage",
                    "fruit drop",
                    "wilting"
            ]
        
        # query damage_reports table
        with self.engine.connect() as conn:
            freq_df = pd.read_sql(
                "SELECT report_id, COUNT(*) as freq FROM damage_reports GROUP BY plant_id", 
                conn
            )
    
        # extract data from query results 
        valid_rep_id = freq_df["report_id"].to_list()
        freqs = freq_df["freq"].to_list()
        id_weights = np.array(freqs) / np.sum(freqs)
        chosen_rep_id = np.random.choice(valid_rep_id, size=n_obs, p=id_weights)
    
        # query inspectors table
        with self.engine.connect() as conn:
            valid_insp_id = pd.read_sql("SELECT inspector_id FROM inspectors", conn)
    
        valid_insp_id = valid_insp_id["inspector_id"].to_list()
        # append data
        data = []
        for _ in range(n_obs):
            data.append({
                "report_id": random.choice(chosen_rep_id),
                "inspector_id": random.choice(valid_insp_id),
                "notes": fk.sentence(ext_word_list=word_list, nb_words=1),
                "inspection_date": random.choice(random.choices(date, weights=weights, k=20)).date()
            })
        df = pd.DataFrame(data)
        
        return df


    def gen_inspectors_data(self, n_obs):
        """
        Generate inspectors records with uneven  distribution.
        
        Args:
            n_obs (int): Number of inspectors records to generate.
            engine : SQLAlchemy engine object.
        
        Returns:
            pd.DataFrame: Generated inspectors records.
        """
        
        random.seed(42)
        fk = Faker("en_NG")
        region = ["Northern-belt", "Middle-belt", "Southern-belt"]
        contact = self.gen_phone_number(n_obs)
        data = []
        
        for _ in range(n_obs):
            data.append({
            "name": fk.name(),
            "region": random.choice(region),
            "contact": random.choice(contact)    
        })
        
        df = pd.DataFrame(data)
        return df

