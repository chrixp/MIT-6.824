export LC_COLLATE="C"
rm -f results.txt
cat mr-out-* | sort | more > results.txt
