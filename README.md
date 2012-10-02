mysql2wrapper
=============

very simple mysql2 wrapper.
ore ore library!

gitup(){
if [ -n "$1" ]; then
   git add . ; git commit -a -m "$1"; git push;
else
   echo "コミットメッセージを入れてちょーだい"
fi
}
