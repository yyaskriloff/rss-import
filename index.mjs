import { config } from 'dotenv'
config()
import rss from 'rss-to-json'
import sharp from 'sharp'
import pLimit from 'p-limit'
import fs from 'node:fs'
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3'
import path from 'node:path'
import ffmpeg from 'fluent-ffmpeg'
import postgres from 'postgres'
import { drizzle } from 'drizzle-orm/postgres-js'
import {
  pgTable,
  bigserial,
  varchar,
  bigint,
  timestamp,
  boolean,
  integer,
  text
} from 'drizzle-orm/pg-core'

const dbClient = postgres(process.env.DATABASE)
const db = drizzle(dbClient)
import { sql, eq } from 'drizzle-orm'

export const episodes = pgTable('episodes', {
  id: bigserial('id', { mode: 'number' }).notNull(),
  showId: bigint('show_id', { mode: 'number' }),
  imageUrl: varchar('image_url'),
  description: text('description'),
  storageUrl: varchar('storage_url').notNull(),
  link: varchar('link'),
  createdAt: timestamp('created_at', {
    withTimezone: true,
    mode: 'date'
  }).defaultNow(),
  updatedAt: timestamp('updated_at', { withTimezone: true, mode: 'date' }),
  position: integer('position'),
  title: varchar('title'),
  publishDate: timestamp('publish_date', {
    withTimezone: true,
    mode: 'date'
  }),
  deleted: boolean('deleted').default(false),
  subtitle: varchar('subtitle'),
  season: integer('season'),
  duration: varchar('duration'),
  tags: varchar('tags').default('{}').array(),
  categories: varchar('categories').default('{}').array(),
  status: varchar('status'),
  storageUsed: bigint('storage_used', { mode: 'number' }),
  lastEditor: bigint('last_editor', { mode: 'number' }),
  compressionStatus: varchar('compression_status', { length: 32 }).default(
    'new'
  ),
  nginxImageUrl: varchar('nginx_image_url'),
  adsStatus: varchar('ads_status', { length: 64 }).default('new'),
  originalUrl: varchar('original_url'),
  // You can use { mode: "bigint" } if numbers are exceeding js number limitations
  originalFileSize: bigint('original_file_size', { mode: 'number' }),
  originalDuration: varchar('original_duration')
})

const limit = pLimit(35)
const Bucket = process.env.BUCKET
const s3 = new S3Client({ region: 'us-east-1' })
const url = process.argv[2]
const showId = process.argv[3]
let ownerID = process.argv[4]
const makeKey = ext => `protected/${ownerID}/${Date.now()}.${ext}`
let DEFAULT_IMAGE = ''

const compressImage = async imageUrl =>
  fetch(imageUrl)
    .then(res => res.arrayBuffer())
    .then(image =>
      sharp(image)
        .jpeg({ quality: 100, progressive: true })
        .resize(1403, 1403, {
          kernel: sharp.kernel.nearest,
          fit: 'contain',
          position: 'center',
          background: {
            r: 0,
            g: 0,
            b: 0,
            alpha: 0.5
          }
        })
        .toFormat('jpeg')
        .toBuffer()
    )

export const uploadToS3 = async (file, key, type) => {
  if (file instanceof Buffer) {
    const putCommand = new PutObjectCommand({
      Bucket,
      Key: key,
      Body: file,
      Tagging: 'compressed=true',
      StorageClass: 'STANDARD_IA',
      //   ContentType: 'audio/mpeg'
      ContentType: type
    })

    await s3.send(putCommand)
    return
  }

  const compressedObject = fs.readFileSync(file)

  const putCommand = new PutObjectCommand({
    Bucket,
    Key: key,
    Body: compressedObject,
    Tagging: 'compressed=true',
    StorageClass: 'STANDARD_IA',
    // ContentType: 'audio/mpeg'
    ContentType: type
  })

  await s3.send(putCommand)
}

const proccessEpisode = imageUrl => (item, index) =>
  limit(async () => {
    if (!item.enclosures || !item.enclosures[0])
      throw new Error("Item doesn't have enclosure")
    const imageKey = DEFAULT_IMAGE
    // imageUrl !== item.itunes_image.href
    //   ? DEFAULT_IMAGE
    //   : await compressImage(item.itunes_image.href).then(
    //       async outputBuffer => {
    //         const key = makeKey('jpg')
    //         await uploadToS3(outputBuffer, key, 'image/jpeg')
    //         return key
    //       }
    //     )

    const audioUrl = item.enclosures[0].url
    const audioLength = item.enclosures[0].length

    const bufferArray = await fetch(audioUrl).then(res => res.arrayBuffer())

    // remove query params from audioUrl
    const cleanAudioUrl = audioUrl.split('?')[0]

    const splits = cleanAudioUrl.split('.')
    const fileType = splits[splits.length - 1]

    const pathDate = Date.now()
    const inputPath = path.join('/tmp', pathDate + '.' + fileType)
    const outputPath = path.join('/tmp', pathDate + 'output' + '.mp3')

    fs.writeFileSync(inputPath, new Uint8Array(bufferArray), 'binary')

    await new Promise((resolve, reject) => {
      ffmpeg()
        .input(inputPath)
        .audioCodec('libmp3lame')
        .audioBitrate('64k')
        .audioChannels(1)
        .audioFrequency(44100)
        .format('mp3')
        .on('end', () => resolve())
        .on('error', reject)
        .save(outputPath)
    })

    const audioSize = fs.statSync(outputPath).size

    fs.unlinkSync(inputPath)

    const audioKey = makeKey('mp3')
    await uploadToS3(fs.readFileSync(outputPath), audioKey, 'audio/mpeg')
    fs.unlinkSync(outputPath)
    return {
      title: item.title,
      showId,
      description: '<p>' + item.description + '</p>',
      imageUrl: 'https://s3.jewishpodcasts.fm/' + imageKey,
      storageUrl:
        'https://records.jewishpodcasts.fm/' +
        audioKey +
        `?show_id=${showId}&episode_id=`,
      position: index + 1,
      publishDate: new Date(item.published),
      createdAt: new Date(item.created),
      season: 1,
      duration: audioLength,
      deleted: false,
      status: 'published',
      compressionStatus: 'compressed',
      storageUsed: audioSize,
      nginxImageUrl: ('https://s3.jewishpodcasts.fm/' + imageKey).replace(
        'protected',
        'img'
      ),
      originalUrl: 'https://jewishpodcasts-prod.s3.amazonaws.com/' + audioKey,
      originalFileSize: audioSize,
      originalDuration: audioLength
    }
  })

const main = async () => {
  const rssFeed = await rss.parse(url, {
    validateStatus: function (status) {
      return true
    }
  })

  const show = await db
    .execute(sql`SELECT * FROM shows WHERE id = ${showId}`)
    .then(res => res[0])

  ownerID = show.owner_id
  DEFAULT_IMAGE = show.image_url.replace(
    'https://jewishpodcasts-prod.s3.amazonaws.com/',
    ''
  )

  const episodeMapFunction = proccessEpisode(rssFeed.image)

  console.log('Total episodes to process: ', rssFeed.items.length)

  const exacutables = rssFeed.items.reverse().map((item, index) =>
    episodeMapFunction(item, index)
      .then(res => {
        console.log('Processed episode ', item.title)
        return res
      })
      .catch(() => {
        console.log('Could not process episode ', item.title)
        return null
      })
  )

  const episodesToAdd = await Promise.all(exacutables)

  const newEpisodes = await db
    .insert(episodes)
    .values(episodesToAdd.filter(Boolean))
    .returning()

  await Promise.all(
    newEpisodes.map(async episode =>
      db
        .update(episodes)
        .set({
          storageUrl: episode.storageUrl + episode.id
        })
        .where(eq(episodes.id, episode.id))
    )
  )

  await dbClient.end()
}

await main()
