import os
import certifi
from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING

print("🚀 Iniciando conexión Mongo...")

# Cargar variables de entorno
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME", "clima_data")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "clima_data")

if not MONGO_URI:
    raise ValueError("❌ No existe MONGO_URI en el .env")

print("📦 DB_NAME:", DB_NAME)
print("📚 COLLECTION_NAME:", COLLECTION_NAME)
print("🔐 Certifi CA:", certifi.where())

# Crear cliente Mongo seguro (Atlas TLS)
client = MongoClient(
    MONGO_URI,
    tls=True,
    tlsCAFile=certifi.where(),
    serverSelectionTimeoutMS=8000,
    connectTimeoutMS=8000,
    socketTimeoutMS=8000,
)

try:
    # Ping para verificar conexión
    print("📡 Haciendo ping...")
    print(client.admin.command("ping"))

    print("✅ Conectado correctamente a MongoDB Atlas")

    # Seleccionar base y colección
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    print("📂 Base activa:", db.name)
    print("📄 Colección activa:", collection.name)
    print("📚 Bases disponibles:", client.list_database_names())

    # Crear índice ejemplo (opcional pero profesional)
    collection.create_index([("time", ASCENDING)])

    print("🎯 Índice creado/verificado correctamente")

except Exception as e:
    print("❌ Error de conexión:")
    print(e)