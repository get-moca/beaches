// netlify/functions/webhook.js
// Processes Apify beach data and updates Supabase using URL as primary key

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
            beaches_updated: 0,
            conditions_added: 0,
            skipped_no_url: 0
        };

        // Handle both single objects and arrays
        const items = Array.isArray(beachData) ? beachData : [beachData];

        for (const item of items) {
            try {
                // Skip items without URL (URL is our primary key)
                if (!item.source_url) {
                    console.log('Skipping item without source_url:', item);
                    results.skipped_no_url++;
                    continue;
                }

                // Log what data we have for this beach
                const hasName = !!item.name;
                const hasMunicipality = !!item.municipality;
                const hasOccupancy = !!(item.occupancy_percent_raw || item.occupancy_display_value);
                const hasFlag = !!item.flag_status;
                const hasJellyfish = item.has_jellyfish !== null && item.has_jellyfish !== undefined;

                console.log(`Processing ${item.source_url}:`);
                console.log(`  Data available: name(${hasName}), municipality(${hasMunicipality}), occupancy(${hasOccupancy}), flag(${hasFlag}), jellyfish(${hasJellyfish})`);

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
                    // Insert new beach - only add if we have at least a name or can derive one from URL
                    let beachName = item.name;
                    if (!beachName) {
                        // Derive name from URL as fallback
                        const urlParts = item.source_url.split('/');
                        const lastPart = urlParts[urlParts.length - 1];
                        if (lastPart && lastPart !== 'playa') {
                            beachName = lastPart
                                .replace(/-/g, ' ')
                                .replace(/\b\w/g, l => l.toUpperCase())
                                .trim();
                        }
                    }

                    if (!beachName) {
                        console.log(`Skipping beach - no name and cannot derive from URL: ${item.source_url}`);
                        results.errors++;
                        continue;
                    }

                    const { data: newBeach, error: insertError } = await supabase
                        .from('beaches')
                        .insert({
                            name: beachName,
                            municipality: item.municipality || null,
                            source_url: item.source_url,
                            // latitude and longitude will be added manually later
                        })
                        .select('id')
                        .single();

                    if (insertError) throw insertError;
                    
                    beachId = newBeach.id;
                    results.beaches_added++;
                    console.log(`Added new beach: ${beachName} (ID: ${beachId})`);
                } else {
                    beachId = existingBeach.id;
                    
                    // Update beach name/municipality ONLY if we have new data
                    const updateData = {};
                    if (item.name) updateData.name = item.name;
                    if (item.municipality) updateData.municipality = item.municipality;
                    
                    if (Object.keys(updateData).length > 0) {
                        updateData.updated_at = new Date().toISOString();
                        
                        const { error: updateError } = await supabase
                            .from('beaches')
                            .update(updateData)
                            .eq('id', beachId);

                        if (updateError) throw updateError;
                        
                        results.beaches_updated++;
                        console.log(`Updated beach ${beachId} with: ${Object.keys(updateData).join(', ')}`);
                    }
                }

                // Insert new beach condition - always add, even with null values
                const conditionData = {
                    beach_id: beachId,
                    occupancy_percent_raw: item.occupancy_percent_raw || null,
                    occupancy_display_value: item.occupancy_display_value || null,
                    flag_status: item.flag_status || null,
                    has_jellyfish: item.has_jellyfish !== null && item.has_jellyfish !== undefined ? item.has_jellyfish : null,
                    scraped_at: item.scraped_at || new Date().toISOString()
                };

                const { error: conditionError } = await supabase
                    .from('beach_conditions')
                    .insert(conditionData);

                if (conditionError) throw conditionError;

                results.conditions_added++;
                results.processed++;
                
                // Log what we stored
                const storedData = [];
                if (conditionData.occupancy_percent_raw) storedData.push(`occupancy: ${conditionData.occupancy_percent_raw}`);
                if (conditionData.flag_status) storedData.push(`flag: ${conditionData.flag_status}`);
                if (conditionData.has_jellyfish !== null) storedData.push(`jellyfish: ${conditionData.has_jellyfish}`);
                
                console.log(`Added condition for beach ${beachId}: ${storedData.length > 0 ? storedData.join(', ') : 'no condition data'}`);

            } catch (itemError) {
                console.error(`Error processing item ${item.source_url}:`, itemError);
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
        } else {
            console.log('Cleaned up old condition records (>24h)');
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
