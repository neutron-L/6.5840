# for i in {0..100}; 
# do go test -race -run 3A; 
# done 

# for i in {0..100}; 
# do go test -race -run 3B; 
# done 



for i in {0..100}; 
do 
 go test -run 3C >> 3C.log
 go test -run 3D >> 3D.log 
done 


for i in {0..100}; 
do go test -run 3A >> 3A.log; 
done 


for i in {0..100}; 
do go test -run 3B >> 3B.log; 
done 
