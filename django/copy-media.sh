# Clear /media/uploads folder
rm -rf /media/uploads/*

# Copy video-test files to folders 1 to 10 within /media/uploads
for i in {1..10}; do
    mkdir -p /media/uploads/$i
    cp -r /home/my-user/app/core/fixtures/media/video-test/* /media/uploads/$i
done

# Copy thumbnails to /media/uploads directly without creating subfolders
cp -r /home/my-user/app/core/fixtures/media/thumbnails/* /media/uploads/
