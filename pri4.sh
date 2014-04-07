psql -d warehouse -h mdw -p 5432 -U usermin -f small.sql -o output.txt & 
psql -d warehouse -h mdw -p 5432 -U usermin -f small.sql -o output.txt & 
psql -d warehouse -h mdw -p 5432 -U usermax -f small.sql -o output.txt &
psql -d warehouse -h mdw -p 5432 -U usermax -f small.sql -o output.txt & 
