# Make sure the mapr user and group exist
getent passwd mapr > /dev/null 2>&1
if [ $? -ne 0 ]; then
	echo "Required user mapr does not exist" >&2
	exit 1
fi
getent group mapr > /dev/null 2>&1
if [ $? -ne 0 ]; then
	echo "Required group mapr does not exist" >&2
	exit 2
fi
