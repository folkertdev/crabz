use std::{fs::File, io::IoSlice, mem::MaybeUninit, path::Path, sync::mpsc};

use zlib_rs::{
    deflate::{DeflateConfig, Method, Strategy},
    DeflateFlush, ReturnCode,
};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    let deflate_config = zlib_rs::deflate::DeflateConfig {
        level: 6,
        method: Method::Deflated,
        window_bits: 15,
        mem_level: DEF_MEM_LEVEL,
        strategy: Strategy::Default,
    };

    let input_path = Path::new("/tmp/silesia-small.tar");
    let input_file = File::open(input_path).unwrap();
    let mmap = unsafe { memmap2::Mmap::map(&input_file) }.unwrap();

    let mut output_path = input_path.to_owned();
    let new_extension = match output_path.extension() {
        Some(ext) => format!("{}.gz", ext.to_string_lossy()),
        None => "gz".to_string(),
    };
    output_path.set_extension(new_extension);
    let mut output_file = File::options()
        .write(true)
        .create(true)
        .open(&output_path)
        .unwrap();

    compress_slice_parallel(&mut output_file, &mmap, deflate_config).unwrap();

    // validate that the compression is correct
    if false {
        let input = std::fs::read(input_path).unwrap();
        let result = std::fs::read(output_path).unwrap();

        let mut output = vec![0; input.len() * 2];

        let inflate_config = zlib_rs::inflate::InflateConfig {
            window_bits: deflate_config.window_bits,
        };

        let (output, err) =
            zlib_rs::inflate::uncompress_slice(&mut output, &result, inflate_config);
        assert_eq!(err, ReturnCode::Ok);

        assert_eq!(output, input);
    }
}

// this seems optimal. Making it smaller means more communication overhead, so is slower. Making it
// bigger doesn't seem to really help.
const CHUNK_SIZE: usize = 128 * 1024;

fn compress_slice_parallel(
    output_file: &mut (impl std::io::Write + std::marker::Send),
    slice: &[u8],
    deflate_config: DeflateConfig,
) -> std::io::Result<()> {
    let threads = 8;
    let chunk_count = slice.len().div_ceil(CHUNK_SIZE);

    let (mut job_sender, job_receiver) = spmc::channel();
    let (complete_sender, complete_receiver) = mpsc::channel();
    let (buffer_sender, buffer_receiver) = mpsc::channel();

    // this is kind of annoying: we try to reduce memory consumption here but in practice that
    // throttles performance. I am not sure why this is. Real pigz gets away with much lower memory
    // use.
    for _ in 0..16 * threads {
        buffer_sender.send(vec![]).unwrap();
    }

    let chunks = slice.chunks(CHUNK_SIZE);

    // the last 32kb of every chunk serves as the initial window for the next chunk
    let windows = slice
        .chunks_exact(CHUNK_SIZE)
        .map(|chunk| &chunk[chunk.len() - 32 * 1024..]);
    let windows = [&[][..]].into_iter().chain(windows);

    std::thread::scope(move |spawner| {
        spawner.spawn(move || {
            writer(
                output_file,
                chunk_count,
                deflate_config,
                complete_receiver,
                buffer_sender,
            )
        });

        for _ in 0..threads {
            let job_receiver = job_receiver.clone();
            let complete_sender = complete_sender.clone();

            spawner.spawn(move || worker(job_receiver, complete_sender));
        }

        drop(job_receiver);

        for (index, (data, window)) in chunks.zip(windows).enumerate() {
            let output_buffer = buffer_receiver.recv().unwrap();

            let job = Job::Compress {
                index,
                data,
                window,
                config: deflate_config,
                output_buffer,
            };

            job_sender.send(job).unwrap();

            match Mode::from_config(&deflate_config) {
                Mode::Raw => { /* raw deflate does not calculate a checksum */ }
                Mode::Standard => {
                    let job = Job::CalculateAdler32 { index, data };
                    job_sender.send(job).unwrap();
                }
                Mode::Gzip => {
                    let job = Job::CalculateCrc32 { index, data };
                    job_sender.send(job).unwrap();
                }
            }
        }

        // all jobs have been sent;
        // workers will now shut down when the channel is empty
        drop(job_sender);
    });

    Ok(())
}

enum Job<'a> {
    CalculateAdler32 {
        index: usize,
        data: &'a [u8],
    },
    CalculateCrc32 {
        index: usize,
        data: &'a [u8],
    },
    Compress {
        index: usize,
        data: &'a [u8],
        window: &'a [u8],
        config: DeflateConfig,
        output_buffer: Vec<u8>,
    },
}

enum Complete {
    Adler32 {
        index: usize,
        checksum: (usize, u32),
    },
    Crc32 {
        index: usize,
        checksum: (usize, u32),
    },
    Compressed {
        index: usize,
        output_buffer: Vec<u8>,
    },
}

enum Mode {
    Raw,
    Standard,
    Gzip,
}

impl Mode {
    fn from_config(deflate_config: &DeflateConfig) -> Self {
        if (1..16).contains(&deflate_config.window_bits) {
            Mode::Standard
        } else if (16..).contains(&deflate_config.window_bits) {
            Mode::Gzip
        } else {
            Mode::Raw
        }
    }
}

fn worker(receiver: spmc::Receiver<Job>, sender: mpsc::Sender<Complete>) {
    while let Ok(job) = receiver.recv() {
        match job {
            Job::CalculateAdler32 { index, data } => {
                let adler = zlib_rs::adler32(1, data);

                let complete = Complete::Adler32 {
                    index,
                    checksum: (data.len(), adler as u32),
                };

                sender.send(complete).unwrap();
            }
            Job::CalculateCrc32 { index, data } => {
                let crc = zlib_rs::crc32(0, data);

                let complete = Complete::Crc32 {
                    index,
                    checksum: (data.len(), crc as u32),
                };

                sender.send(complete).unwrap();
            }
            Job::Compress {
                index,
                data,
                window,
                config,
                mut output_buffer,
            } => {
                output_buffer.resize(zlib_rs::deflate::compress_bound(data.len()), 0);

                let deflated = process_chunk(&mut output_buffer, data, window, config);
                let bytes_written = deflated.len();

                output_buffer.truncate(bytes_written);

                let complete = Complete::Compressed {
                    index,
                    output_buffer,
                };

                sender.send(complete).unwrap();
            }
        }
    }
}

// thread that performs the writing of the output
fn writer(
    file: &mut impl std::io::Write,
    chunk_count: usize,
    config: DeflateConfig,
    receiver: mpsc::Receiver<Complete>,
    sender: mpsc::Sender<Vec<u8>>,
) -> std::io::Result<()> {
    let mode = Mode::from_config(&config);

    match mode {
        Mode::Raw => { /* no header */ }
        Mode::Standard => {
            // write the correct gzip header into our result
            let mut header_buffer = [0; 32];
            let (_, err) = zlib_rs::deflate::compress_slice_with_flush(
                &mut header_buffer,
                b"",
                config,
                DeflateFlush::Finish,
            );
            assert_eq!(err, ReturnCode::Ok);

            // size of the zlib header
            file.write_all(&header_buffer[..2])?;
        }
        Mode::Gzip => {
            // write the correct gzip header into our result
            let mut header_buffer = [0; 32];
            let (_, err) = zlib_rs::deflate::compress_slice_with_flush(
                &mut header_buffer,
                b"",
                config,
                DeflateFlush::Finish,
            );
            assert_eq!(err, ReturnCode::Ok);

            // size of the gzip header
            file.write_all(&header_buffer[..10])?;
        }
    }

    let mut chunks = vec![None; chunk_count];
    let mut checksums = vec![(0, 0); chunk_count];

    let mut next_chunk_to_write = 0;

    while let Ok(complete) = receiver.recv() {
        match complete {
            Complete::Adler32 { index, checksum } => {
                debug_assert_eq!(checksums[index], (0, 0));
                checksums[index] = checksum;
            }
            Complete::Crc32 { index, checksum } => {
                debug_assert_eq!(checksums[index], (0, 0));
                checksums[index] = checksum;
            }
            Complete::Compressed {
                index,
                output_buffer: chunk,
            } => {
                if index != next_chunk_to_write {
                    chunks[index] = Some(chunk);
                } else {
                    let mut io_slices_buf = [IoSlice::new(&[]); 32];
                    let mut next_io_slice = 1;

                    io_slices_buf[0] = IoSlice::new(chunk.as_slice());

                    let mut it = chunks[next_chunk_to_write + 1..].iter();
                    while let Some(Some(chunk)) = it.next() {
                        io_slices_buf[next_io_slice] = IoSlice::new(chunk.as_slice());
                        next_io_slice += 1;

                        if next_io_slice == io_slices_buf.len() {
                            file.write_vectored(&io_slices_buf)?;
                            next_chunk_to_write += io_slices_buf.len();
                            next_io_slice = 0;
                        }
                    }

                    file.write_vectored(&io_slices_buf[..next_io_slice])?;
                    next_chunk_to_write += next_io_slice;

                    // send the buffer back so it can be re-used
                    // this is allowed to fail (i.e. not send) when the
                    // channel is already closed.
                    let _ = sender.send(chunk);
                }
            }
        }
    }

    // add a final block (that has the `is_last_block` flag set) and the checksum and length check
    let mut trailer = [0x3, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0];

    // write the checksum (if required)
    match mode {
        Mode::Raw => { /* raw deflate does not calculate a checksum */ }
        Mode::Standard => {
            let mut len = 0;
            let mut adler1 = 1;
            for (len2, adler2) in checksums {
                adler1 = zlib_rs::adler32_combine(adler1, adler2, len2 as u64);
                len += len2;
            }

            trailer[2..][..4].copy_from_slice(&adler1.to_be_bytes());
            trailer[6..][..4].copy_from_slice(&(len as u32).to_le_bytes());

            file.write_all(&trailer)?;
        }
        Mode::Gzip => {
            let mut len = 0;
            let mut crc1 = 0;
            for (len2, crc2) in checksums {
                crc1 = zlib_rs::crc32_combine(crc1, crc2, len2 as u64);
                len += len2;
            }

            trailer[2..][..4].copy_from_slice(&crc1.to_le_bytes());
            trailer[6..][..4].copy_from_slice(&(len as u32).to_le_bytes());

            file.write_all(&trailer)?;
        }
    }

    Ok(())
}

pub const MAX_WBITS: i32 = 15; // 32kb LZ77 window
const MAX_MEM_LEVEL: i32 = 9;
const DEF_MEM_LEVEL: i32 = if MAX_MEM_LEVEL > 8 { 8 } else { MAX_MEM_LEVEL };

fn process_chunk<'a>(
    output: &'a mut [u8],
    chunk: &[u8],
    dictionary: &[u8],
    mut config: DeflateConfig,
) -> &'a mut [u8] {
    config.window_bits = match Mode::from_config(&config) {
        Mode::Raw => config.window_bits,
        Mode::Standard => -1 * config.window_bits,
        Mode::Gzip => -1 * (config.window_bits - 16),
    };

    let (prefix, err) = 'blk: {
        let flush = DeflateFlush::SyncFlush;
        let output_uninit = unsafe {
            core::slice::from_raw_parts_mut(
                output.as_mut_ptr() as *mut MaybeUninit<u8>,
                output.len(),
            )
        };

        {
            use zlib_rs::{
                c_api::z_stream,
                deflate::{deflate, end, init, DeflateStream},
            };

            let mut stream = z_stream {
                next_in: chunk.as_ptr() as *mut u8,
                avail_in: 0, // for special logic in the first  iteration
                total_in: 0,
                next_out: output_uninit.as_mut_ptr() as *mut u8,
                avail_out: 0, // for special logic on the first iteration
                total_out: 0,
                msg: core::ptr::null_mut(),
                state: core::ptr::null_mut(),
                zalloc: None,
                zfree: None,
                opaque: core::ptr::null_mut(),
                data_type: 0,
                adler: 0,
                reserved: 0,
            };

            let err = init(&mut stream, config);
            if err != ReturnCode::Ok {
                break 'blk (&mut [][..], err);
            }

            let err = if let Some(stream) = unsafe { DeflateStream::from_stream_mut(&mut stream) } {
                zlib_rs::deflate::set_dictionary(stream, dictionary)
            } else {
                ReturnCode::StreamError
            };

            if err != ReturnCode::Ok {
                break 'blk (&mut [][..], err);
            }

            let max = core::ffi::c_uint::MAX as usize;

            let mut left = output_uninit.len();
            let mut source_len = chunk.len();

            loop {
                if stream.avail_out == 0 {
                    stream.avail_out = Ord::min(left, max) as _;
                    left -= stream.avail_out as usize;
                }

                if stream.avail_in == 0 {
                    stream.avail_in = Ord::min(source_len, max) as _;
                    source_len -= stream.avail_in as usize;
                }

                let flush = if source_len > 0 {
                    DeflateFlush::NoFlush
                } else {
                    flush
                };

                let err =
                    if let Some(stream) = unsafe { DeflateStream::from_stream_mut(&mut stream) } {
                        deflate(stream, flush)
                    } else {
                        ReturnCode::StreamError
                    };

                if err != ReturnCode::Ok {
                    break;
                }
            }

            // SAFETY: we have now initialized these bytes
            let output_slice = unsafe {
                core::slice::from_raw_parts_mut(
                    output_uninit.as_mut_ptr() as *mut u8,
                    stream.total_out as usize,
                )
            };

            // may DataError if insufficient output space
            let return_code =
                if let Some(stream) = unsafe { DeflateStream::from_stream_mut(&mut stream) } {
                    match end(stream) {
                        Ok(_) => ReturnCode::Ok,
                        Err(_) => ReturnCode::DataError,
                    }
                } else {
                    ReturnCode::Ok
                };

            (output_slice, return_code)
        }
    };
    assert_eq!(err, ReturnCode::DataError);

    prefix
}
