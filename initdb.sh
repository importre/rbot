gzip -d db/$1.gz
dropdb $1 --if-exists
createdb $1
createuser -r deploy
createuser -r postgres
psql $1 < db/$1
