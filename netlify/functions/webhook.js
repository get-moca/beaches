// netlify/functions/webhook.js
// Processes Apify beach data and updates Supabase

const { createClient } = require('@supabase/supabase-js');

exports.handler = async (event, context) => {
    // CORS headers
    const headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'POST, OPTIONS',
        'Content-Type': 'application/json'
    };

    // Handle preflight requests
    if (event.httpMethod === 'OPTIONS') {
        return { statusCode: 200, headers, body: '' };
    }

    if (event.httpMethod !== 'POST') {
        return {
            statusCode: 405,
            headers,
            body: JSON.stringify({ error: 'Method not allowed' })
        };
    }

    try {
        // Initialize Supabase client
        const supabase = createClient(
            process.env.SUPABASE_URL,
            process.env.SUPABASE_SERVICE_ROLE_KEY // Use service role key for full access
        );

        // Parse the webhook payload from Apify
        const apifyWebhook = JSON.parse(event.body);
        console.log('Received Apify webhook:', apifyWebhook);

        // Extract dataset ID from the run resource
        const datasetId = apifyWebhook.resource?.defaultDatasetId;
        
        if (!datasetId) {
            throw new Error('No dataset ID found in webhook payload');
        }

        console.log('Fetching data from dataset:', datasetId);

        // Fetch the actual scraped data from Apify dataset
        const datasetUrl = `https://api.apify.com/v2/datasets/${datasetId}/items`;
        const response = await fetch(datasetUrl);
        
        if (!response.ok) {
            throw new Error(`Failed to fetch dataset: ${response.status} ${response.statusText}`);
        }

        const beachData = await response.json();
        console.log(`Fetched ${beachData.length} beach records from Apify`);

        // Process the beach data
        const results = {
            processed: 0,
            errors: 0,
            beaches_added: 0,
            conditions_added: 0
        };

        // Handle both single objects and arrays
        const items = Array.isArray(beachData) ? beachData : [beachData];

        for (const item of items) {
            try {
                // Skip empty items
                if (!item.name || !item.source_url) {
                    console.log('Skipping item without name or source_url:', item);
                    results.errors++;
                    continue;
                }

                // Log data completeness
                const dataFields = ['occupancy_percent_raw', 'flag_status', 'has_jellyfish'];
                const missingFields = dataFields.filter(field => item[field] === null || item[field] === undefined);
                if (missingFields.length > 0) {
                    console.log(`Beach ${item.name} missing data for: ${missingFields.join(', ')}`);
                }

                // Check if beach exists by source_url
                const { data: existingBeach, error: findError } = await supabase
                    .from('beaches')
                    .select('id')
                    .eq('source_url', item.source_url)
                    .single();

                if (findError && findError.code !== 'PGRST116') { // PGRST116 = no rows returned
                    throw findError;
                }

                let beachId;

                if (!existingBeach) {
                    // Insert new beach
                    const { data: newBeach, error: insertError } = await supabase
                        .from('beaches')
                        .insert({
                            name: item.name,
                            municipality: item.municipality,
                            source_url: item.source_url,
                            // latitude and longitude will be added manually later
                        })
                        .select('id')
                        .single();

                    if (insertError) throw insertError;
                    
                    beachId = newBeach.id;
                    results.beaches_added++;
                    console.log(`Added new beach: ${item.name} (ID: ${beachId})`);
                } else {
                    beachId = existingBeach.id;
                    
                    // Update beach name/municipality in case they changed
                    const { error: updateError } = await supabase
                        .from('beaches')
                        .update({
                            name: item.name,
                            municipality: item.municipality,
                            updated_at: new Date().toISOString()
                        })
                        .eq('id', beachId);

                    if (updateError) throw updateError;
                }

                // Insert new beach condition
                const { error: conditionError } = await supabase
                    .from('beach_conditions')
                    .insert({
                        beach_id: beachId,
                        occupancy_percent_raw: item.occupancy_percent_raw,
                        occupancy_display_value: item.occupancy_display_value,
                        flag_status: item.flag_status,
                        has_jellyfish: item.has_jellyfish,
                        air_temperature: item.air_temperature,
                        water_temperature: item.water_temperature,
                        wind_speed: item.wind_speed,
                        wave_height: item.wave_height,
                        scraped_at: item.scraped_at || new Date().toISOString()
                    });

                if (conditionError) throw conditionError;

                results.conditions_added++;
                results.processed++;
                
                console.log(`Updated conditions for beach: ${item.name}`);

            } catch (itemError) {
                console.error(`Error processing item ${item.name}:`, itemError);
                results.errors++;
            }
        }

        // Clean up old condition records (keep only last 24 hours)
        const twentyFourHoursAgo = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
        const { error: cleanupError } = await supabase
            .from('beach_conditions')
            .delete()
            .lt('scraped_at', twentyFourHoursAgo);

        if (cleanupError) {
            console.error('Cleanup error:', cleanupError);
        }

        console.log('Processing complete:', results);

        return {
            statusCode: 200,
            headers,
            body: JSON.stringify({
                success: true,
                message: 'Beach data processed successfully',
                results
            })
        };

    } catch (error) {
        console.error('Webhook error:', error);
        return {
            statusCode: 500,
            headers,
            body: JSON.stringify({
                success: false,
                error: error.message
            })
        };
    }
};
