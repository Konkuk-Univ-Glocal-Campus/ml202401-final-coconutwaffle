import time
import pickle
import pandas as pd
import logging
from datetime import datetime, timedelta
import signal
import sys
import langid
import requests
import certifi

# Logger setup
logger = logging.getLogger('TranslationLogger')
logger.setLevel(logging.INFO)

if not logger.handlers:
    # File handler setup
    file_handler = logging.FileHandler('translation_log.log', mode='a', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Console handler setup
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)


count = 0
total_count = 0
TEST_MODE = False
res = None
last_backup_time = datetime.now()
fail_ctl = 0





def handle_exit(sig, frame):
    global file_handler, logger
    logger.removeHandler(file_handler)
    file_handler.close()
    logger.info('*'*40+"\nProgram interrupted, saving progress...")
    save_progress(res)
    sys.exit(0)

# Setup signal handler
signal.signal(signal.SIGINT, handle_exit)


def deepl(text, dst_lang='en'):
    global res
    key ='15c0fa04-1559-4687-864b-9a50eeaf682f:fx'
    url = 'https://api-free.deepl.com/v2/translate'
    params = {'auth_key' : key, 'text' : text, "target_lang": 'EN'}
    req = requests.post(url, data=params, verify=certifi.where())
    try:
        if req.status_code != 200:
            raise RuntimeError('Request Fail')
        req_text = req.json()["translations"][0]['text']
    except Exception as e:
        print(e)
        print(req.text)
        save_progress(res)
        sys.exit(0)
    return req_text

    

def trans(text, sr_lang='en', dst_lang='en'):
    global count, total_count, translator, logger
    count += 1
    total_count += 1
    if TEST_MODE:
        return text + ' in ' + sr_lang + ' fake translate complete'
    else:
        try:
            logger.info(f"Translating: {text}")
            tl = deepl(text, dst_lang=dst_lang)
            #tl = translator.translate(text, src='auto' ,dest=dst_lang).text
            logger.info(f"Complete: {tl}")
            return tl
        except Exception as e:
            logger.error(f"Error in translating :[{type(text)}]<->[{text}]<->[{e}]")
            raise RuntimeError(f'TL Fail: type={type(text)}, value={text}') from e



def save_progress(data):
    global logger
    if TEST_MODE:
        with open('translation_test.pkl', 'wb') as f:
            pickle.dump(data, f)
            logger.info(f"BackUp: {len(data['org'])}, {data['org'][-1]}, {data['tl'][-1]}")
    else:
        with open('translation_progress.pkl', 'wb') as f:
            pickle.dump(data, f)
            logger.info(f"BackUp: {len(data['org'])}, {data['org'][-1]}, {data['tl'][-1]}")

def safe_translate(text, dst_lang):
    global res, last_backup_time, translator, fail_ctl
    try:
        translated_text = trans(text, dst_lang=dst_lang)
    except Exception as e:
        logger.error(f'trans Error: [{e}]')
        fail_ctl+=1
        if fail_ctl>=5:
            logger.error('연속된 실패: API 호출 제한 가능성')
            input()
            fail_ctl=0
            logger.error('프로그램을 재시작')
        if translated_text not in locals() :
            raise RuntimeError('translated_text가 없음') from e
            

    if datetime.now() - last_backup_time > timedelta(seconds=20):
        save_progress(res)  # 진행 상태 저장
        last_backup_time = datetime.now()

    return translated_text



def load_progress():
    if TEST_MODE:
        try:
            with open('translation_test.pkl', 'rb') as f:
                return pickle.load(f)
        except FileNotFoundError:
            return {'org': [], 'tl': []}
    else:
        try:
            with open('translation_progress.pkl', 'rb') as f:
                return pickle.load(f)
        except FileNotFoundError:
            return {'org': [], 'tl': []}
        



def check_res(dst_lang='en'):
    global res, translator
    try:
        new_org = []
        new_tl = []
        for org, tl in zip(res['org'], res['tl']):
            detected_lang = langid.classify(tl)[0]
            if detected_lang != dst_lang:
                logger.info(f"Detected as {detected_lang}: {tl} ")
                

            if detected_lang == dst_lang:
                new_org.append(org)
                new_tl.append(tl)
            else:
                logger.info(f"Removed translation: {tl}")

        res['org'] = new_org
        res['tl'] = new_tl
    except Exception as e:
        raise RuntimeError('check_res Fail') from e


def translate_reviews_and_update_dataframe(dataframe, target_lang='en'):
    global count, total_count, res, logger, translator

    res = load_progress()
    #check_res()
    
    if 'review_title_tr' not in dataframe.columns:
        dataframe['review_title_tr'] = pd.NA
    if 'review_text_tr' not in dataframe.columns:
        dataframe['review_text_tr'] = pd.NA

    for index, row in dataframe.iterrows():
        original_title = row['review_title']
        original_text = row['review_text']
        location = row.get('location', '')

        if "united states" in location.lower():
            dataframe.at[index, 'review_title_tr'] = original_title
            dataframe.at[index, 'review_text_tr'] = original_text
            continue

        if original_title not in res['org']:
            try:
                detected_lang = langid.classify(original_title)[0]
                if detected_lang=='en':
                    translated_title = original_title
                else:
                    translated_title = safe_translate(original_title, dst_lang=target_lang)
                res['tl'].append(translated_title)
                res['org'].append(original_title)
            except Exception as e:
                logger.error(f"Translation failed for title at index {index}: [{e}]")
                save_progress(res)
                continue

        if original_text not in res['org']:
            try:
                detected_lang = langid.classify(original_title)[0]
                if detected_lang=='en':
                    translated_text = original_title
                else:
                    translated_text = safe_translate(original_text, dst_lang=target_lang)
                res['tl'].append(translated_text)
                res['org'].append(original_text)
            except Exception as e:
                logger.error(f"Translation failed for text at index {index}: [{e}]")
                save_progress(res)
                continue

    save_progress(res)

    
    retry_failed_translations(dataframe, target_lang, 20)

    for org, tl in zip(res['org'], res['tl']):
        dataframe['review_text_tr'] = dataframe['review_text'].replace(org, tl)
        dataframe['review_title_tr'] = dataframe['review_title'].replace(org, tl)
    
    return dataframe

def retry_failed_translations(dataframe, target_lang, count=0):
    global translator

    if count<=0:
        return dataframe
    recount =0


    for index, row in dataframe.iterrows():
        original_title = row['review_title']
        original_text = row['review_text']
        location = row.get('location', '')

        if "united states" in location.lower():
            dataframe.at[index, 'review_title_tr'] = original_title
            dataframe.at[index, 'review_text_tr'] = original_text
            continue

        if original_title not in res['org']:
            try:
                recount+=1
                translated_title = safe_translate(original_title, dst_lang=target_lang)
                res['tl'].append(translated_title)
                res['org'].append(original_title)
            except Exception as e:
                save_progress(res)
                logger.error(f"Translation failed for title at index {index}: [{e}]")
                continue

        if original_text not in res['org']:
            try:
                translated_text = safe_translate(original_text, target_lang)
                res['tl'].append(translated_text)
                res['org'].append(original_text)
            except Exception as e:
                save_progress(res)
                logger.error(f"Translation failed for text at index {index}: [{e}]")
                continue
    
    if recount != 0:
        save_progress(res)
        retry_failed_translations(dataframe, target_lang, count-1)





def translate_all_review_and_title(data):
    # Translate and update the DataFrame
    try:
        updated_df = translate_reviews_and_update_dataframe(data)
        updated_df.to_pickle('updated_data.pkl')  # Save the updated dataframe
        logger.info('All items successfully translated and original data updated with new translated columns.')
    except Exception as e:
        logger.error(f"Error during the translation process: {e}")
        raise e
    
    return updated_df

if __name__=='__main__':
    # Load the dataset
    data = pd.read_pickle('data.pkl')
    translate_all_review_and_title(data)