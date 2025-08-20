// netlify/functions/webhook.js - UPDATED FOR EXACT SCHEMA MATCH
const { createClient } = require('@supabase/supabase-js')

const supabaseUrl = process.env.SUPABASE_URL
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY
const supabase = createClient(supabaseUrl, supabaseServiceKey)

exports.handler = async (event, context) => {
    if (event.httpMethod !== 'POST') {
        return {
            statusCode: 405,
            body: JSON.stringify({ message: 'Method not allowed' })
        }
    }

    try {
        console.log('📥 Received Apify webhook for beach data')
        
        const body = JSON.parse(event.body)
        const datasetId = body.resource?.defaultDatasetId
        
        if (!datasetId) {
            return {
                statusCode: 400,
                body: JSON.stringify({ message: 'No dataset ID found' })
            }
        }
        
        console.log(`📊 Fetching beach data from dataset: ${datasetId}`)
        
        const apifyResponse = await fetch(`https://api.apify.com/v2/datasets/${datasetId}/items?format=json`)
        const apifyData = await apifyResponse.json()
        
        if (!Array.isArray(apifyData)) {
            return {
                statusCode: 400,
                body: JSON.stringify({ message: 'No valid beach data found' })
            }
        }

        console.log(`📊 Processing ${apifyData.length} beaches`)

        let created = 0
        let updated = 0
        let errors = 0

        for (const beachRecord of apifyData) {
            try {
                console.log(`Processing: ${beachRecord.name}`)
                
                if (!beachRecord.name) {
                    console.log('⚠️ Skipping record without name')
                    continue
                }
                
                // Create clean place_id from beach name
                const placeId = beachRecord.name
                    .toLowerCase()
                    .normalize('NFD')
                    .replace(/[\u0300-\u036f]/g, '') // Remove accents
                    .replace(/\s+/g, '_')
                    .replace(/[^a-z0-9_]/g, '')
                    .replace(/_+/g, '_')
                    .replace(/^_|_$/g, '')
                
                // Check if beach already exists by name (more reliable than place_id)
                let { data: existingBeach, error: findError } = await supabase
                    .from('beaches')
                    .select('id')
                    .eq('name', beachRecord.name)
                    .single()

                let beachId

                if (existingBeach) {
                    // Beach exists - just get the ID
                    beachId = existingBeach.id
                    console.log(`✅ Found existing beach: ${beachRecord.name}`)
                    
                    // Update municipality if we have new data
                    if (beachRecord.municipality) {
                        await supabase
                            .from('beaches')
                            .update({ 
                                municipality: beachRecord.municipality,
                                updated_at: new Date().toISOString()
                            })
                            .eq('id', beachId)
                    }
                    
                } else {
                    // Beach doesn't exist - create it
                    const { data: newBeach, error: insertError } = await supabase
                        .from('beaches')
                        .insert({
                            place_id: placeId,
                            name: beachRecord.name,
                            municipality: beachRecord.municipality || null,
                            description: `Beach in ${beachRecord.municipality || 'Mallorca'}`,
                            // Leave lat/lng null - you'll add manually later
                            latitude: null,
                            longitude: null
                        })
                        .select('id')
                        .single()

                    if (insertError) {
                        console.error(`❌ Error creating beach ${beachRecord.name}:`, insertError)
                        errors++
                        continue
                    }

                    beachId = newBeach.id
                    created++
                    console.log(`🆕 Created new beach: ${beachRecord.name}`)
                }

                // Always insert NEW condition record (creates history)
                const { error: conditionsError } = await supabase
                    .from('beach_conditions')
                    .insert({
                        beach_id: beachId,
                        occupancy_percent: beachRecord.occupancy_percent,
                        flag_status: beachRecord.flag_status || 'green',
                        has_jellyfish: Boolean(beachRecord.has_jellyfish),
                        recorded_at: beachRecord.scraped_at || new Date().toISOString(),
                        source: 'apify_scraper'
                    })

                if (conditionsError) {
                    console.error(`❌ Error inserting conditions for ${beachRecord.name}:`, conditionsError)
                    errors++
                } else {
                    console.log(`🌊 Added conditions for: ${beachRecord.name}`)
                    updated++
                }

            } catch (beachError) {
                console.error(`❌ Error processing ${beachRecord.name}:`, beachError)
                errors++
            }
        }

        console.log(`✅ Processing complete:`)
        console.log(`  - ${created} new beaches created`)
        console.log(`  - ${updated} condition records added`)
        console.log(`  - ${errors} errors`)
        
        return {
            statusCode: 200,
            body: JSON.stringify({ 
                message: 'Beach data processed successfully',
                created: created,
                updated: updated,
                errors: errors,
                total: apifyData.length
            })
        }

    } catch (error) {
        console.error('❌ Webhook failed:', error)
        return {
            statusCode: 500,
            body: JSON.stringify({ 
                message: 'Internal server error',
                error: error.message 
            })
        }
    }
}
