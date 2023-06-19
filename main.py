from collectors.zoom import Zoom
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")

z = Zoom(client)

z.get_zoom_records(
    "https://zoom.us/rec/share/4CkpeBjt0nayyaCj2HSfwOMuSaC8i8XBCWY4RtigSbCL7s2LXsHC6W9r3vqqIG66.YZgT-lMNj1W1l4_C",
    "yeardream23!",
    False,
)
