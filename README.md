# mdb-importer-ffprobe

# work with python 3.5+

git clone https://github.com/Bnei-Baruch/mdb-importer-ffprobe.git
cd mdb-importer-ffprobe
python3 -m venv env
. env/bin/activate
pip install -r requirements.txt

# to put files that exist in postgres but not in couchbase to couchbase, run
python3 main.py

# to write the csv for video , run
python3 to_csv_video.py

# to write the csv for audio, run
python3 to_csv_audio.py
