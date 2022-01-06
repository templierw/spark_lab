
repeat(){
	for i in $( seq 1 $1 ); do echo -n "#"; done
    echo
}

repeat $1