// netlify/functions/webhook.js
const { createClient } = require('@supabase/supabase-js')

const supabaseUrl = process.env.SUPABASE_URL
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY

if (!supabaseUrl || !supabaseServiceKey) {
    console.error('❌ Missing Supabase environment variables')
}

const supabase = createClient(supabaseUrl, supabaseServiceKey)

exports.handler = async (event, context) => {
    // Only allow POST requests
    if (event.httpMethod !== 'POST') {
        return {
            statusCode: 405,
            body: JSON.stringify({ message: 'Method not allowed' })
        }
    }

    try {
        console.log('📥 Received Apify webhook')
        
        // Parse the webhook body
        const body = JSON.parse(event.body)
        console.log('Raw webhook body:', JSON.stringify(body, null, 2))
        
        // Get beach data from Apify webhook
        // The data is in eventData.data for Apify webhooks
        const apifyData = body.eventData?.data || body.data || []
        
        if (!Array.isArray(apifyData)) {
            console.log('❌ No valid array data found')
            return {
                statusCode: 400,
                body: JSON.stringify({ message: 'No valid beach data found' })
            }
        }

        console.log(`📊 Processing ${apifyData.length} beach records`)

        let processed = 0
        let errors = 0

        // Process each beach from Apify
        for (const beachRecord of apifyData) {
            try {
                console.log(`Processing beach: ${beachRecord.name}`)
                
                // Create place_id from beach name
                const placeId = beachRecord.name
                    .toLowerCase()
                    .replace(/\s+/g, '_')
                    .replace(/[^a-z0-9_]/g, '')
                
                // First, check if beach exists
                let { data: existingBeach, error: findError } = await supabase
                    .from('beaches')
                    .select('id')
                    .eq('place_id', placeId)
                    .single()

                let beachId

                if (existingBeach) {
                    beachId = existingBeach.id
                    console.log(`✅ Found existing beach: ${beachRecord.name}`)
                } else {
                    // Create new beach
                    const { data: newBeach, error: insertError } = await supabase
                        .from('beaches')
                        .insert({
                            place_id: placeId,
                            name: beachRecord.name,
                            latitude: parseFloat(beachRecord.latitude),
                            longitude: parseFloat(beachRecord.longitude),
                            municipality: beachRecord.municipality || 'Unknown'
                        })
                        .select('id')
                        .single()

                    if (insertError) {
                        console.error(`❌ Error creating beach ${beachRecord.name}:`, insertError)
                        errors++
                        continue
                    }

                    beachId = newBeach.id
                    console.log(`🆕 Created new beach: ${beachRecord.name}`)
                }

                // Insert current conditions
                const { error: conditionsError } = await supabase
                    .from('beach_conditions')
                    .insert({
                        beach_id: beachId,
                        occupancy_percent: parseInt(beachRecord.occupancy_percent) || 0,
                        flag_status: beachRecord.flag_status || 'green',
                        has_jellyfish: Boolean(beachRecord.has_jellyfish),
                        recorded_at: beachRecord.scraped_at || new Date().toISOString(),
                        source: 'apify'
                    })

                if (conditionsError) {
                    console.error(`❌ Error inserting conditions for ${beachRecord.name}:`, conditionsError)
                    errors++
                } else {
                    console.log(`🌊 Updated conditions for: ${beachRecord.name}`)
                    processed++
                }

            } catch (beachError) {
                console.error(`❌ Error processing beach ${beachRecord.name}:`, beachError)
                errors++
            }
        }

        console.log(`✅ Webhook complete: ${processed} processed, ${errors} errors`)
        
        return {
            statusCode: 200,
            body: JSON.stringify({ 
                message: 'Beach data processed successfully',
                processed: processed,
                errors: errors,
                total: apifyData.length
            })
        }

    } catch (error) {
        console.error('❌ Webhook handler failed:', error)
        
        return {
            statusCode: 500,
            body: JSON.stringify({ 
                message: 'Internal server error',
                error: error.message 
            })
        }
    }
}
