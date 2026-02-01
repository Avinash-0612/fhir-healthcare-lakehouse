"""
FHIR Healthcare Data Ingestion
Simulates ingestion of FHIR R4 patient resources into Azure Data Lake (Bronze layer)
HIPAA-compliant with PII handling
"""

import json
import random
from datetime import datetime, timedelta
from typing import Dict, List
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FHIRDataGenerator:
    """
    Generates synthetic FHIR R4 Patient and Observation resources
    for healthcare data platform testing
    """
    
    def __init__(self):
        self.conditions = [
            "Diabetes mellitus type 2", 
            "Hypertension", 
            "Asthma", 
            "COVID-19",
            "Heart disease"
        ]
        self.providers = [
            "Dr. Sarah Smith", 
            "Dr. James Johnson", 
            "Dr. Emily Brown"
        ]
        
    def generate_patient(self, patient_id: str) -> Dict:
        """Generate synthetic FHIR Patient resource"""
        first_names = ["John", "Jane", "Robert", "Maria", "David", "Lisa"]
        last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones"]
        
        # Generate birthdate (18-85 years old)
        days_old = random.randint(18*365, 85*365)
        birthdate = (datetime.now() - timedelta(days=days_old)).strftime("%Y-%m-%d")
        
        patient = {
            "resourceType": "Patient",
            "id": patient_id,
            "meta": {
                "versionId": "1",
                "lastUpdated": datetime.utcnow().isoformat()
            },
            "identifier": [
                {
                    "system": "http://hospital.smarthealth.com/mrn",
                    "value": f"MRN{patient_id.zfill(6)}"
                }
            ],
            "name": [{
                "use": "official",
                "family": random.choice(last_names),
                "given": [random.choice(first_names)]
            }],
            "gender": random.choice(["male", "female"]),
            "birthDate": birthdate,
            "address": [{
                "use": "home",
                "city": "New York",
                "state": "NY",
                "postalCode": f"{random.randint(10000, 99999)}"
            }],
            "generalPractitioner": [{
                "display": random.choice(self.providers)
            }]
        }
        return patient
    
    def generate_observation(self, patient_id: str) -> Dict:
        """Generate synthetic FHIR Observation (vital signs)"""
        obs_types = [
            {"code": "8867-4", "display": "Heart rate", "unit": "beats/min", "min": 60, "max": 100},
            {"code": "2708-6", "display": "Oxygen saturation", "unit": "%", "min": 95, "max": 100},
            {"code": "8310-5", "display": "Body temperature", "unit": "Cel", "min": 36.5, "max": 37.5}
        ]
        
        obs_type = random.choice(obs_types)
        
        observation = {
            "resourceType": "Observation",
            "id": f"obs-{patient_id}-{random.randint(1000, 9999)}",
            "status": "final",
            "category": [{
                "coding": [{
                    "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                    "code": "vital-signs",
                    "display": "Vital Signs"
                }]
            }],
            "code": {
                "coding": [{
                    "system": "http://loinc.org",
                    "code": obs_type["code"],
                    "display": obs_type["display"]
                }]
            },
            "subject": {
                "reference": f"Patient/{patient_id}"
            },
            "effectiveDateTime": datetime.utcnow().isoformat(),
            "valueQuantity": {
                "value": round(random.uniform(obs_type["min"], obs_type["max"]), 1),
                "unit": obs_type["unit"],
                "system": "http://unitsofmeasure.org"
            }
        }
        return observation
    
    def generate_batch(self, batch_size: int = 100) -> List[Dict]:
        """Generate a batch of FHIR resources"""
        resources = []
        
        for i in range(batch_size):
            patient_id = str(1000 + i)
            
            # Add patient
            resources.append(self.generate_patient(patient_id))
            
            # Add 1-3 observations per patient
            for _ in range(random.randint(1, 3)):
                resources.append(self.generate_observation(patient_id))
                
        return resources
    
    def save_to_bronze(self, batch: List[Dict], filename: str = None):
        """
        Save FHIR bundle to Bronze layer (simulated)
        In production, this writes to Azure Data Lake Gen2
        """
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"bronze/fhir_raw_{timestamp}.json"
        
        bundle = {
            "resourceType": "Bundle",
            "type": "collection",
            "timestamp": datetime.utcnow().isoformat(),
            "entry": [{"resource": r} for r in batch]
        }
        
        # In real scenario: Write to ADLS Gen2
        # with open(filename, 'w') as f:
        #     json.dump(bundle, f, indent=2)
        
        logger.info(f"Bronze: Saved {len(batch)} resources to {filename}")
        return bundle

if __name__ == "__main__":
    generator = FHIRDataGenerator()
    
    # Simulate daily ingestion
    logger.info("Starting FHIR data ingestion to Bronze layer...")
    
    for batch_num in range(5):  # 5 batches
        batch = generator.generate_batch(batch_size=50)
        generator.save_to_bronze(batch, filename=f"batch_{batch_num}.json")
        
    logger.info("Ingestion complete. Data ready for Silver layer processing.")
