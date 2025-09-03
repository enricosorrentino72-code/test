import requests
import base64
import json

# Carica l'immagine e codificala in base64
with open("C:\\Users\\Enrico Sorrentino\\OneDrive - WBA\\Desktop\\buttami\\Buttami\\Test\\imange\\Rifiuti_camion_abusivo.png", "rb") as image_file:
    encoded_image = base64.b64encode(image_file.read()).decode('utf-8')

print("Imange loaded!!")
generic_prompt="Analyze the image and provide a detailed description of what is visible, including the environment, possible location, people, animals, and objects. Describe the relationships or interactions between the elements, the mood or emotions conveyed, and any relevant contextual clues that might help understand the scene."
plant_prompt="Assess the physiological condition of the plant in the image. Look for symptoms of chlorosis, wilting, necrosis, fungal or insect damage. Include observations on hydration, sunlight exposure, and plant vigor. Provide possible diagnoses and remediation steps."
abusive_prompt="""
You are an expert in detecting illegal waste dumping in unauthorized areas such as countryside, roadsides, urban spaces, abandoned industrial zones, or public areas not designated for waste disposal.

You will receive an input image. Your task is to:

Analyze the visual content of the image.

--Determine whether the image clearly shows a scene of illegal waste dumping in a non-authorized location (e.g., visible piles of garbage, people unloading items, parked or active vehicles like vans or trucks, garbage bags, bulky or hazardous waste).

--Assess the context: are there any signs, bins, or disposal containers? Or does the location appear clearly inappropriate (e.g., woods, roadsides, abandoned lots)?

Important context: The camera is installed in a location where any kind of waste dumping is strictly prohibited. 
Therefore, if you observe any type of waste being dumped or already present, the situation must be considered illegal.

Your output must always be in this format:

    1.Answer: YES or NO

    2.Explanation: A brief explanation (1–3 sentences) of why you chose that answer, based only on visible evidence in the image.

Important rules:

You must choose either YES or NO.

Do not answer UNCERTAIN.

Do not invent details that are not visible in the image. Base your judgment only on what you can clearly see.

"""
# Prepara i dati della richiesta
data = {
    "model": "llava:7b",
    "prompt": abusive_prompt,
    "images": [encoded_image],
     "stream": True
}
print("Data loaded!!")

# Invia la richiesta al server Ollama (in esecuzione in locale)
response = requests.post("http://localhost:11434/api/generate", json=data,stream=True)

print("Response Done!!")

# Visualizza la risposta
output = ""
for line in response.iter_lines():
    if line:
        try:
            json_line = json.loads(line.decode('utf-8'))
            output += json_line.get("response", "")
        except json.JSONDecodeError as e:
            print("Errore nel parsing JSON:", e)
            print("Linea ricevuta:", line.decode('utf-8'))

print("\n🧠 Risposta generata da LLaVA:")
print(output)
