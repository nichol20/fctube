import { VideoModel } from "../../../models";

export async function getVideo(slug: string): Promise<VideoModel> {
  const response = await fetch(`${process.env.DJANGO_API_URL}/videos/${slug}`, {
    //cache: "no-cache",
    next: {
    //     revalidate: 60 * 5
        tags: [`video-${slug}`]
    }
  });
  return response.json();
}

