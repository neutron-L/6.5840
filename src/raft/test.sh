# for i in {0..100}; 
# do go test -race -run 3A; 
# done 

# for i in {0..100}; 
# do go test -race -run 3B; 
# done 

for i in {0..1000}; 
do go test -race -run 3C; 
done 
