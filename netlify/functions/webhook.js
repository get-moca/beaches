// netlify/functions/webhook.js - UPDATE ONLY VERSION
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
        console.log('📥 Received Apify webhook for beach conditions update')
        
        const body = JSON.parse(event.body)
        const datasetId = body.resource?.defaultDatasetId
        
        if (!datasetId) {
            return {
                statusCode: 400,
                body: JSON.stringify({ message: 'No dataset ID found' })
            }
        }
        
        console.log(`📊 Fetching beach conditions from dataset: ${datasetId}`)
        
        const apifyResponse = await fetch(`https://api.apify.com/v2/datasets/${datasetId}/items?format=json`)
        const apifyData = await apifyResponse.json()
        
        if (!Array.isArray(apifyData)) {
            return {
                statusCode: 400,
                body: JSON.stringify({ message: 'No valid beach data found' })
            }
        }

        console.log(`📊 Processing conditions for ${apifyData.length} beaches`)

        let updated = 0
        let notFound = 0
        let errors = 0

        for (const beachRecord of apifyData) {
            try {
                console.log(`Updating conditions for: ${beachRecord.name}`)
                
                // Find existing beach by name (you'll add lat/lng manually)
                const { data: existingBeach, error: findError } = await supabase
                    .from('beaches')
                    .select('id')
                    .ilike('name', `%${beachRecord.name}%`)
                    .single()

                if (!existingBeach) {
                    console.log(`⚠️ Beach not found in database: ${beachRecord.name}`)
                    notFound++
                    continue
                }

                // Insert NEW condition record (don't update existing)
                const { error: conditionsError } = await supabase
                    .from('beach_conditions')
                    .insert({
                        beach_id: existingBeach.id,
                        occupancy_percent: beachRecord.occupancy_percent || null,
                        flag_status: beachRecord.flag_status || 'green',
                        has_jellyfish: Boolean(beachRecord.has_jellyfish),
                        water_temperature: beachRecord.water_temperature || null,
                        air_temperature: beachRecord.air_temperature || null,
                        wind_speed: beachRecord.wind_speed || null,
                        wave_height: beachRecord.wave_height || null,
                        recorded_at: beachRecord.scraped_at || new Date().toISOString(),
                        source: 'apify_live'
                    })

                if (conditionsError) {
                    console.error(`❌ Error updating conditions for ${beachRecord.name}:`, conditionsError)
                    errors++
                } else {
                    console.log(`✅ Updated conditions for: ${beachRecord.name}`)
                    updated++
                }

            } catch (beachError) {
                console.error(`❌ Error processing ${beachRecord.name}:`, beachError)
                errors++
            }
        }

        console.log(`✅ Conditions update complete: ${updated} updated, ${notFound} not found, ${errors} errors`)
        
        return {
            statusCode: 200,
            body: JSON.stringify({ 
                message: 'Beach conditions updated successfully',
                updated: updated,
                notFound: notFound,
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
