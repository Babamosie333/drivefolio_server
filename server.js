import express from 'express'
import { WebSocketServer } from 'ws'
import mongoose from 'mongoose'
import { createServer } from 'http'
import { v4 as uuidv4 } from 'uuid'
import dotenv from 'dotenv'

dotenv.config()

// ═══════════════════════════════════
// DATABASE MODELS
// ═══════════════════════════════════

// Whisper Model
const whisperSchema = new mongoose.Schema({
    id:        { type: String, default: uuidv4 },
    message:   { type: String, required: true, maxlength: 30 },
    country:   { type: String, default: 'us' },
    x:         { type: Number, required: true },
    z:         { type: Number, required: true },
    createdAt: { type: Date, default: Date.now }
})
const Whisper = mongoose.model('Whisper', whisperSchema)

// Circuit Score Model
const scoreSchema = new mongoose.Schema({
    id:        { type: String, default: uuidv4 },
    time:      { type: Number, required: true },
    country:   { type: String, default: 'us' },
    createdAt: { type: Date, default: Date.now }
})
const Score = mongoose.model('Score', scoreSchema)

// Cataclysm Model
const cataclysmSchema = new mongoose.Schema({
    count:    { type: Number, default: 0 },
    progress: { type: Number, default: 0 }
})
const Cataclysm = mongoose.model('Cataclysm', cataclysmSchema)

// Cookie Model
const cookieSchema = new mongoose.Schema({
    count: { type: Number, default: 0 }
})
const Cookie = mongoose.model('Cookie', cookieSchema)

// ═══════════════════════════════════
// SERVER SETUP
// ═══════════════════════════════════

const app = express()
const server = createServer(app)
const wss = new WebSocketServer({ server })

app.use(express.json())

// Health check
app.get('/', (req, res) =>
{
    res.json({ status: 'Drivefolio server running!', clients: wss.clients.size })
})

// ═══════════════════════════════════
// DATA CACHE
// ═══════════════════════════════════

let cache = {
    whispers:        [],
    scores:          [],
    cataclysmCount:  0,
    cataclysmProgress: 0,
    cookiesCount:    0,
}

// Load data from DB into cache
const loadCache = async () =>
{
    try
    {
        // Whispers
        cache.whispers = await Whisper
            .find()
            .sort({ createdAt: -1 })
            .limit(parseInt(process.env.MAX_WHISPERS) || 30)

        // Scores
        cache.scores = await Score
            .find()
            .sort({ time: 1 })
            .limit(20)

        // Cataclysm
        let cataclysm = await Cataclysm.findOne()
        if(!cataclysm)
            cataclysm = await Cataclysm.create({ count: 0, progress: 0 })
        cache.cataclysmCount    = cataclysm.count
        cache.cataclysmProgress = cataclysm.progress

        // Cookies
        let cookie = await Cookie.findOne()
        if(!cookie)
            cookie = await Cookie.create({ count: 0 })
        cache.cookiesCount = cookie.count

        console.log('Cache loaded successfully')
    }
    catch(error)
    {
        console.error('Cache load error:', error)
    }
}

// ═══════════════════════════════════
// BROADCAST TO ALL CLIENTS
// ═══════════════════════════════════

const broadcast = (data, excludeClient = null) =>
{
    const message = JSON.stringify(data)
    wss.clients.forEach(client =>
    {
        if(client !== excludeClient && client.readyState === 1)
            client.send(message)
    })
}

// ═══════════════════════════════════
// WEBSOCKET EVENTS
// ═══════════════════════════════════

wss.on('connection', async (ws) =>
{
    console.log(`Client connected. Total: ${wss.clients.size}`)

    // Send init data to new client
    ws.send(JSON.stringify({
        type:              'init',
        whispers:          cache.whispers,
        scores:            cache.scores,
        cataclysmCount:    cache.cataclysmCount,
        cataclysmProgress: cache.cataclysmProgress,
        cookiesCount:      cache.cookiesCount,
    }))

    // Handle messages
    ws.on('message', async (rawData) =>
    {
        try
        {
            const data = JSON.parse(rawData)

            // ── Whisper Insert ──
            if(data.type === 'whisperInsert')
            {
                const whisper = await Whisper.create({
                    id:      uuidv4(),
                    message: data.message.slice(0, 30),
                    country: data.country || 'us',
                    x:       data.x || 0,
                    z:       data.z || 0,
                })

                cache.whispers.unshift(whisper)

                // Keep max whispers
                const max = parseInt(process.env.MAX_WHISPERS) || 30
                if(cache.whispers.length > max)
                {
                    const removed = cache.whispers.splice(max)
                    for(const w of removed)
                        await Whisper.deleteOne({ id: w.id })
                }

                broadcast({
                    type:    'whisperInsert',
                    whisper: whisper
                })
            }

            // ── Score Insert ──
            else if(data.type === 'scoreInsert')
            {
                const score = await Score.create({
                    id:      uuidv4(),
                    time:    data.time,
                    country: data.country || 'us',
                })

                cache.scores.push(score)
                cache.scores.sort((a, b) => a.time - b.time)
                cache.scores = cache.scores.slice(0, 20)

                broadcast({
                    type:   'scoresUpdate',
                    scores: cache.scores
                })
            }

            // ── Cataclysm Insert ──
            else if(data.type === 'cataclysmInsert')
            {
                cache.cataclysmCount++
                cache.cataclysmProgress = Math.min(cache.cataclysmCount / 1000, 1)

                await Cataclysm.findOneAndUpdate(
                    {},
                    {
                        count:    cache.cataclysmCount,
                        progress: cache.cataclysmProgress
                    },
                    { upsert: true }
                )

                broadcast({
                    type:              'cataclysmUpdate',
                    cataclysmCount:    cache.cataclysmCount,
                    cataclysmProgress: cache.cataclysmProgress,
                })
            }

            // ── Cookies Insert ──
            else if(data.type === 'cookiesInsert')
            {
                cache.cookiesCount += data.amount || 1

                await Cookie.findOneAndUpdate(
                    {},
                    { count: cache.cookiesCount },
                    { upsert: true }
                )

                broadcast({
                    type:         'cookiesUpdate',
                    cookiesCount: cache.cookiesCount,
                })
            }
        }
        catch(error)
        {
            console.error('Message error:', error)
        }
    })

    ws.on('close', () =>
    {
        console.log(`Client disconnected. Total: ${wss.clients.size}`)
    })

    ws.on('error', (error) =>
    {
        console.error('WebSocket error:', error)
    })
})
// Keep server awake on Render free plan
const SERVER_URL = process.env.RENDER_EXTERNAL_URL
if(SERVER_URL)
{
    setInterval(async () =>
    {
        try
        {
            await fetch(SERVER_URL)
            console.log('Keep alive ping sent')
        }
        catch(error)
        {
            console.error('Keep alive failed:', error)
        }
    }, 10 * 60 * 1000) // Every 10 minutes
}
// ═══════════════════════════════════
// START SERVER
// ═══════════════════════════════════

const start = async () =>
{
    try
    {
        // Connect to MongoDB
        await mongoose.connect(process.env.MONGODB_URI)
        console.log('MongoDB connected!')

        // Load cache
        await loadCache()

        // Start server
        const PORT = process.env.PORT || 3000
        server.listen(PORT, () =>
        {
            console.log(`Server running on port ${PORT}`)
        })
    }
    catch(error)
    {
        console.error('Server start error:', error)
        process.exit(1)
    }
}

start()